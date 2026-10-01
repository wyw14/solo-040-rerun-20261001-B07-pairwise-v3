package interlock

import (
	"errors"
	"fmt"
	"sync"

	"github.com/wyw14/cry-96/internal/model"
)

type Reservation struct {
	ChamberID string              `json:"chamber_id"`
	Direction model.Direction     `json:"direction"`
	Owner     string              `json:"owner"`
	Command   model.DeviceCommand `json:"command"`
}

// Arbiter atomically checks and reserves a chamber direction.
type Arbiter struct {
	mu     sync.Mutex
	active map[string]Reservation
}

func NewArbiter() *Arbiter { return &Arbiter{active: make(map[string]Reservation)} }

// Reserve atomically validates ownership, runs commit and records the
// reservation. While a reservation is in flight, a request from a different
// owner is rejected — even for the same direction — so duplicates or
// concurrent requests can never override the in-flight operation. A retry
// from the same owner for the same direction is recognized as the original
// request and returned without committing again.
func (a *Arbiter) Reserve(chamberID string, direction model.Direction, owner string, commit func() (model.DeviceCommand, error)) (Reservation, error) {
	if chamberID == "" || !direction.Valid() || owner == "" || commit == nil {
		return Reservation{}, errors.New("reservation requires chamber, direction, owner and commit")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if current, ok := a.active[chamberID]; ok {
		if current.Owner == owner && current.Direction == direction {
			return current, nil
		}
		if current.Direction != direction {
			return Reservation{}, errors.New("opposed gate direction is already reserved")
		}
		return Reservation{}, fmt.Errorf("chamber %s %s gate operation is already reserved by another owner", chamberID, direction)
	}
	command, err := commit()
	if err != nil {
		return Reservation{}, err
	}
	reservation := Reservation{ChamberID: chamberID, Direction: direction, Owner: owner, Command: command}
	a.active[chamberID] = reservation
	return reservation, nil
}

func (a *Arbiter) Release(chamberID, owner string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	current, ok := a.active[chamberID]
	if !ok || current.Owner != owner {
		return false
	}
	delete(a.active, chamberID)
	return true
}

func (a *Arbiter) Current(chamberID string) (Reservation, bool) {
	a.mu.Lock()
	reservation, ok := a.active[chamberID]
	a.mu.Unlock()
	return reservation, ok
}
