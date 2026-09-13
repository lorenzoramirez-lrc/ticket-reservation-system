-- ============================================
-- Events
-- ============================================

CREATE TABLE events (
    event_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

-- ============================================
-- Event occurrences
-- ============================================

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

-- ============================================
-- Reservations
-- ============================================

CREATE TABLE reservations (
    reservation_id VARCHAR(50) PRIMARY KEY,
    occurrence_id VARCHAR(50) NOT NULL,
    client_id VARCHAR(100) NOT NULL,
    seat_count INTEGER NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',

    CONSTRAINT fk_reservation_occurrence
        FOREIGN KEY (occurrence_id)
        REFERENCES event_occurrences(occurrence_id),

    CONSTRAINT chk_seat_count_positive
        CHECK (seat_count > 0),

    CONSTRAINT chk_reservation_status
        CHECK (status IN ('ACTIVE', 'CANCELLED'))
);
