package persistence

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

const DB = "data.json"
const WALPath = "wal.log"
const WALMax = 1250

type WAL struct {
	Timestamp string
	Command string
	Key string
	Value string
}

type Persister interface {
	Load() (map[string]string, error)
	Save(map[string]string) error
}

type JSONFilePersister struct {
	Path string
}

func (p *JSONFilePersister) Load() (map[string]string, error) {
	data := make(map[string]string)

	file, err := os.ReadFile(p.Path)
	if err != nil {
		if os.IsNotExist(err) {
			return data, nil
		}
		return nil, err
	}

	if len(file) == 0 {
		return data, nil
	}

	if err := json.Unmarshal(file, &data); err != nil {
		return nil, err
	}
	return data, nil
}

func (p *JSONFilePersister) Save(data map[string]string) error {
	encoded, err := json.MarshalIndent(data, "", " ")
	if err != nil {
		return err
	}

	return os.WriteFile(p.Path, encoded, 0644)
}


type WALLoader struct {
	WALPath string
	WALIndex int64
	WALMax int64
	WALChan chan struct{}
}

func parseWAL(file []byte) ([]WAL, int64, error) {
	lines := strings.Split(strings.TrimSpace(string(file)), "\n")
	lineCount := 0
	logs := make([]WAL, 0)
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		lineCount++
		fields := strings.Split(line, "\t")

		switch fields[1] {
		case "PUT":
			if len(fields) != 4 {
				return []WAL{}, 0, fmt.Errorf("invalid PUT: %s", line)
			}

			logs = append(logs, WAL{
				Command: "PUT",
				Timestamp: fields[0],
				Key:     fields[2],
				Value:   fields[3],
			})

		case "DELETE":
			if len(fields) != 3 {
				return []WAL{}, 0, fmt.Errorf("invalid DELETE: %s", line)
			}

			logs = append(logs, WAL{
				Command: "DELETE",
				Timestamp: fields[0],
				Key:     fields[2],
			})

		default:
			return []WAL{}, 0, fmt.Errorf("unknown command: %s", fields[0])
		}
	}

	return logs, int64(lineCount), nil
}

func (ld *WALLoader) LoadWAL() ([]WAL, error) {
	logs := []WAL{}

	file, err := os.ReadFile(ld.WALPath)
	if err != nil {
		if os.IsNotExist(err) {
			return logs, nil
		}
		return nil, err
	}

	if len(file) == 0 {
		return logs, nil
	}

	logs, walCount, err := parseWAL(file)
	if err != nil {
		return nil, err
	}

	ld.WALIndex = walCount
	return logs, nil
}


func (ld *WALLoader) AppendLog(timestamp string, command string, key string, value string) error {
	f, err := os.OpenFile(
		ld.WALPath,
		os.O_CREATE|os.O_WRONLY|os.O_APPEND,
		0644,
	)
	if err != nil {
		return err
	}
	defer f.Close()

	if command == "DELETE" {
		_, err = fmt.Fprintf(f, "%s\tDELETE\t%s\n", timestamp, key)
		ld.WALIndex += 1
	} else {
		_, err = fmt.Fprintf(f, "%s\t%s\t%s\t%s\n", timestamp, command, key, value)
		ld.WALIndex += 1
	}
	if ld.WALIndex >= ld.WALMax {
		select {
		case ld.WALChan <- struct{}{}:
			default:
		}
	}
	return err
}

func (ld *WALLoader) TruncateLog() error {
	if _, err := os.Stat(ld.WALPath); os.IsNotExist(err) {
		return nil
	}

	inputFile, err := os.Open(ld.WALPath)
	if err != nil {
		return fmt.Errorf("Failed to open log file: %v", err)
	}

	defer inputFile.Close()

	tempPath := ld.WALPath + ".tmp"
	tempFile, err := os.Create(tempPath)

	if err != nil {
		return fmt.Errorf("Failed to create temporary file: %v", err)
	}
	defer tempFile.Close()

	writer := bufio.NewWriter(tempFile)
	scanner := bufio.NewScanner(inputFile)

	lineCount := 0
	for scanner.Scan() {
		lineCount++
		line := scanner.Text()

		if strings.TrimSpace(line) == "" {
			continue
		}

		fields := strings.Split(line, "\t")
		if len(fields) < 3 {
			continue
		}

		if lineCount >= int(ld.WALMax) {
				_, err := writer.WriteString(line + "\n")
				ld.WALIndex = 0
				if err != nil {
					return fmt.Errorf("Failed to write temp file: %v", err)
				}
		}
	}

	writer.Flush()

	inputFile.Close()
	tempFile.Close()

	err = os.Rename(tempPath, ld.WALPath)
	if err != nil {
		return fmt.Errorf("Failed to replace log file: %v", err)
	}
	return nil
}
