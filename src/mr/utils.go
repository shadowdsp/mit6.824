package mr

import (
	"strconv"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

func GenUUID() (string, error) {
	i, err := uuid.NewRandom()
	if err != nil {
		log.Error("Failed to generate UUID")
		return "", err
	}
	return strconv.Itoa(int(i.ID())), nil
}
