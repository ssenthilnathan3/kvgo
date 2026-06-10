package persistence

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

type WAL struct {
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
}

func parseWAL(file []byte, logs *[]WAL) error {
	lines := strings.Split(strings.TrimSpace(string(file)), "\n")

	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}

		fields := strings.Fields(line)

		switch fields[0] {
		case "PUT":
			if len(fields) != 3 {
				return fmt.Errorf("invalid PUT: %q", line)
			}

			*logs = append(*logs, WAL{
				Command: "PUT",
				Key:     fields[1],
				Value:   fields[2],
			})

		case "DELETE":
			if len(fields) != 2 {
				return fmt.Errorf("invalid DELETE: %q", line)
			}

			*logs = append(*logs, WAL{
				Command: "DELETE",
				Key:     fields[1],
			})

		default:
			return fmt.Errorf("unknown command: %s", fields[0])
		}
	}

	return nil
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

	if err := parseWAL(file, &logs); err != nil {
		return nil, err
	}
	return logs, nil
}


func (ld *WALLoader) AppendLog(command string, key string, value string) error {
	f, err := os.OpenFile(
		ld.WALPath,
		os.O_CREATE|os.O_WRONLY|os.O_APPEND,
		0644,
	)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = fmt.Fprintf(f, "%s %s %s\n", command, key, value)
	return err
}

