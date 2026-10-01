package interlock_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/gate"
	"github.com/wyw14/cry-96/internal/interlock"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

func newCoordinator() (*interlock.Coordinator, *gate.Commands) {
	recorder := journal.NewRecorder(journal.NewMemoryStore(), nil)
	commands := gate.NewCommands()
	controller := gate.NewController(gate.NewState("chamber-1"), commands, recorder, nil)
	coordinator := interlock.NewCoordinator(interlock.DefaultPolicy(), interlock.NewArbiter(), controller, nil, nil)
	return coordinator, commands
}

func testCycle() model.TransitCycle {
	return model.TransitCycle{ID: uuid.New(), ChamberID: "chamber-1", Generation: 1}
}

// Regression: a second operator requesting the same direction must not take
// over the reservation or orphan the in-flight command.
func TestRequestOpenRejectsConflictingOwner(t *testing.T) {
	coordinator, _ := newCoordinator()
	cycle := testCycle()

	first, err := coordinator.RequestOpen(context.Background(), cycle, model.DirectionUpstream, "operator-a")
	if err != nil {
		t.Fatalf("first request: %v", err)
	}

	if _, err := coordinator.RequestOpen(context.Background(), cycle, model.DirectionUpstream, "operator-b"); err == nil {
		t.Fatal("second owner must not acquire the reservation")
	}

	reservation, ok := coordinator.Reservation("chamber-1")
	if !ok {
		t.Fatal("reservation must survive the conflicting request")
	}
	if reservation.Owner != "operator-a" {
		t.Fatalf("reservation owner = %q, want operator-a", reservation.Owner)
	}
	if reservation.Command.ID != first.ID {
		t.Fatalf("in-flight command %s was replaced by %s", first.ID, reservation.Command.ID)
	}

	if err := coordinator.Release("chamber-1", "operator-a"); err != nil {
		t.Fatalf("original owner must be able to release: %v", err)
	}
}

// A legitimate retry from the same owner and direction returns the original
// command instead of issuing a new one.
func TestRequestOpenRetryIsIdempotent(t *testing.T) {
	coordinator, commands := newCoordinator()
	cycle := testCycle()

	first, err := coordinator.RequestOpen(context.Background(), cycle, model.DirectionUpstream, "operator-a")
	if err != nil {
		t.Fatalf("first request: %v", err)
	}
	retry, err := coordinator.RequestOpen(context.Background(), cycle, model.DirectionUpstream, "operator-a")
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if retry.ID != first.ID || retry.Epoch != first.Epoch {
		t.Fatalf("retry issued a new command %+v, want original %+v", retry, first)
	}
	if pending := len(commands.Pending("chamber-1")); pending != 1 {
		t.Fatalf("retry must not queue extra commands, pending = %d", pending)
	}
}

func TestRequestOpenRejectsOpposedDirection(t *testing.T) {
	coordinator, _ := newCoordinator()
	cycle := testCycle()

	if _, err := coordinator.RequestOpen(context.Background(), cycle, model.DirectionUpstream, "operator-a"); err != nil {
		t.Fatalf("first request: %v", err)
	}
	if _, err := coordinator.RequestOpen(context.Background(), cycle, model.DirectionDownstream, "operator-a"); err == nil {
		t.Fatal("opposed direction must be rejected while reserved")
	}
}

func TestRequestOpenConcurrentOwners(t *testing.T) {
	coordinator, commands := newCoordinator()
	cycle := testCycle()

	const owners = 8
	results := make(chan error, owners)
	for i := 0; i < owners; i++ {
		owner := string(rune('a' + i))
		go func() {
			_, err := coordinator.RequestOpen(context.Background(), cycle, model.DirectionUpstream, "operator-"+owner)
			results <- err
		}()
	}
	succeeded := 0
	for i := 0; i < owners; i++ {
		if err := <-results; err == nil {
			succeeded++
		}
	}
	if succeeded != 1 {
		t.Fatalf("concurrent requests: %d succeeded, want exactly 1", succeeded)
	}
	if pending := len(commands.Pending("chamber-1")); pending != 1 {
		t.Fatalf("concurrent requests queued %d commands, want 1", pending)
	}
}

func TestRequestOpenAfterRelease(t *testing.T) {
	coordinator, _ := newCoordinator()
	cycle := testCycle()

	if _, err := coordinator.RequestOpen(context.Background(), cycle, model.DirectionUpstream, "operator-a"); err != nil {
		t.Fatalf("first request: %v", err)
	}
	if err := coordinator.Release("chamber-1", "operator-a"); err != nil {
		t.Fatalf("release: %v", err)
	}
	// The gate is now observed opening, so close it before a new open cycle.
	if err := coordinator.Observe("chamber-1", model.DirectionUpstream, model.GateClosed, time.Now()); err != nil {
		t.Fatalf("observe: %v", err)
	}
	if _, err := coordinator.RequestOpen(context.Background(), cycle, model.DirectionUpstream, "operator-b"); err != nil {
		t.Fatalf("new owner must acquire the reservation after release: %v", err)
	}
}
