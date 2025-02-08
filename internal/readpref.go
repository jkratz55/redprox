package internal

import (
	"errors"
)

type ReadPreference uint8

const (
	// Master indicates reads should occur on the master. When using Master redprox
	// will only perform reads from the master.
	Master ReadPreference = 0
	// Slave indicates reads operations should happen on the slave/replicas. However,
	// if no slaves/replicas are available the master will be used.
	Slave ReadPreference = 1
)

func ParseReadPreference(pref int) (ReadPreference, error) {
	switch pref {
	case 0:
		return Master, nil
	case 1:
		return Slave, nil
	default:
		return Master, errors.New("invalid ReadPreference")
	}
}
