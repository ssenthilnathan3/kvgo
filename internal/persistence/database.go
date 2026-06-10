package persistence

import (
	"encoding/json"
	"os"
)

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
