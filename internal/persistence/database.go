package persistence

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
)

const DB = "data.json"
const WALPath = "wal.log"
const WALMax = 1250

type Entry struct {
	Value string `json:"string"`
	Timestamp int64 `json:"version"`
}

type WAL struct {
	Command string
	Key string
	Value Entry
}

type WALLoader struct {
	WALPath string
	WALIndex int64
	WALMax int64
	WALChan chan struct{}
}

type Persister interface {
	Load() (map[string]Entry, error)
	Save(map[string]Entry) error
}

type JSONFilePersister struct {
	Path string
}

func (p *JSONFilePersister) Load() (map[string]Entry, error) {
	data := make(map[string]Entry)

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

func (p *JSONFilePersister) Save(data map[string]Entry) error {
	encoded, err := json.MarshalIndent(data, "", " ")
	if err != nil {
		return err
	}

	return os.WriteFile(p.Path, encoded, 0644)
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

		if len(fields) < 3 {
			return []WAL{}, 0, fmt.Errorf("invalid WAL entry: %s", line)
		}

		timestamp, err := strconv.ParseInt(fields[0], 10, 64)
		if err != nil {
			return []WAL{}, 0, fmt.Errorf("invalid timestamp in WAL entry: %s", line)
		}

		switch fields[1] {
		case "PUT":
			if len(fields) != 4 {
				return []WAL{}, 0, fmt.Errorf("invalid PUT: %s", line)
			}

			logs = append(logs, WAL{
				Command: "PUT",
				Key:     fields[2],
				Value: Entry{
					Value:     fields[3],
					Timestamp: timestamp,
				},
			})

		case "DELETE":
			if len(fields) < 3 {
				return []WAL{}, 0, fmt.Errorf("invalid DELETE: %s", line)
			}

			logs = append(logs, WAL{
				Command: "DELETE",
				Key:     fields[2],
			})

		default:
			return []WAL{}, 0, fmt.Errorf("unknown command: %s", fields[1])
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

	_, err = fmt.Fprintf(f, "%s\t%s\t%s\t%s\n", timestamp, command, key, value)
	ld.WALIndex += 1
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

	var lines []string
	scanner := bufio.NewScanner(inputFile)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) != "" {
			lines = append(lines, line)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("Error reading log file: %v", err)
	}
	inputFile.Close()

	if len(lines) <= int(ld.WALMax) {
		return nil
	}

	lines = lines[len(lines)-int(ld.WALMax):]

	tempPath := ld.WALPath + ".tmp"
	tempFile, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("Failed to create temporary file: %v", err)
	}

	writer := bufio.NewWriter(tempFile)
	for _, line := range lines {
		if _, err := writer.WriteString(line + "\n"); err != nil {
			return fmt.Errorf("Failed to write temp file: %v", err)
		}
	}
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("Error writing file: %v", err)
	}
	tempFile.Close()

	if err := os.Rename(tempPath, ld.WALPath); err != nil {
		return fmt.Errorf("Failed to replace log file: %v", err)
	}

	ld.WALIndex = int64(len(lines))
	return nil
}
