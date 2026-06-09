package persistence

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/ssenthilnathan3/kvgo/constants"
)


func WritePersist(key string, value string) error {
	file, err := os.OpenFile(constants.DB, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	data := make(map[string]string)

	stat, err := file.Stat()
	if err != nil {
		return err
	}

	if stat.Size() > 0 {
		if err := json.NewDecoder(file).Decode(&data); err != nil && err != io.EOF {
			return fmt.Errorf("failed to parse existing json: %w", err)
		}
	}

	data[key] = value

	if _, err := file.Seek(0, 0); err != nil {
		return err
	}

	if err := file.Truncate(0); err != nil {
		return err
	}

	encoded, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}

	if _, err := file.Write(encoded); err != nil {
		return err
	}

	return nil
}
