-- EVENTS
CREATE TABLE IF NOT EXISTS events (
    event_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

-- OCCURRENCES

CREATE TABLE IF NOT EXISTS event_occurrences (
    occurrence_id VARCHAR(50) PRIMARY KEY,
    event_id VARCHAR(50) NOT NULL,
    date_time TIMESTAMP NOT NULL,
    capacity INTEGER NOT NULL,
    occupied_seats INTEGER NOT NULL DEFAULT 0,

    CONSTRAINT fk_occurrence_event
        FOREIGN KEY (event_id)
        REFERENCES events(event_id),

    CONSTRAINT chk_capacity_positive
        CHECK (capacity > 0),

    CONSTRAINT chk_occupied_seats_valid
        CHECK (
            occupied_seats >= 0
            AND occupied_seats <= capacity
        )
);

-- RESERVATIONS

CREATE TABLE IF NOT EXISTS reservations (
    reservation_id VARCHAR(50) PRIMARY KEY,
    occurrence_id VARCHAR(50) NOT NULL,
    client_id VARCHAR(100) NOT NULL,
    seat_count INTEGER NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT fk_reservation_occurrence
        FOREIGN KEY (occurrence_id)
        REFERENCES event_occurrences(occurrence_id),

    CONSTRAINT chk_seat_count_positive
        CHECK (seat_count > 0),

    CONSTRAINT chk_reservation_status
        CHECK (status IN ('ACTIVE', 'CANCELLED'))
);

CREATE TABLE IF NOT EXISTS processed_requests (
    request_id UUID PRIMARY KEY,
    client_id VARCHAR(100) NOT NULL,
    operation_type INTEGER NOT NULL,
    request_fingerprint TEXT NOT NULL,
    response TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS reservation_modifications (
    request_id UUID PRIMARY KEY,
    reservation_id VARCHAR(50) NOT NULL,
    client_id VARCHAR(100) NOT NULL,
    operation_type VARCHAR(20) NOT NULL CHECK (operation_type IN ('MODIFY_QUANTITY', 'MODIFY_OCCURRENCE')),
    previous_occurrence_id VARCHAR(50) NOT NULL,
    new_occurrence_id VARCHAR(50) NOT NULL,
    previous_seat_count INTEGER NOT NULL CHECK (previous_seat_count > 0),
    new_seat_count INTEGER NOT NULL CHECK (new_seat_count > 0),
    modified_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS idx_reservation_modifications_history
    ON reservation_modifications (reservation_id, modified_at DESC);

