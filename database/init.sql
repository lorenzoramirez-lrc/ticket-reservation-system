-- EVENTS
CREATE TABLE events (
    event_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

-- OCCURRENCES

CREATE TABLE event_occurrences (
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

CREATE TABLE reservations (
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

-- INITIAL EVENTS WITH OCCURRENCES 

INSERT INTO events (event_id, name)
VALUES
    ('E1', 'AVATAR'),
    ('E2', 'FLOW');


INSERT INTO event_occurrences (
    occurrence_id,
    event_id,
    date_time,
    capacity,
    occupied_seats
)
VALUES
    (
        'E1-1',
        'E1',
        '2026-10-20 19:00:00',
        100,
        0
    ),
    (
        'E2-1',
        'E2',
        '2026-10-25 19:00:00',
        100,
        0
    );
