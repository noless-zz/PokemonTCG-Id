CREATE TABLE sets (
    id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    series VARCHAR(255),
    printed_total INT,
    total INT,
    legality JSON,
    ptcgo_code VARCHAR(20),
    release_date DATE,
    updated_at DATETIME,
    symbol_image VARCHAR(500),
    logo_image VARCHAR(500)
);

CREATE TABLE cards_classification (
    id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    supertype VARCHAR(50),
    set_id VARCHAR(20),
    number VARCHAR(20),
    rarity VARCHAR(50),
    FOREIGN KEY (set_id) REFERENCES sets(id)
);

CREATE TABLE cards_subtypes (
    card_id VARCHAR(20),
    subtype VARCHAR(50),
    FOREIGN KEY (card_id) REFERENCES cards_classification(id),
    PRIMARY KEY (card_id, subtype)
);

CREATE TABLE cards_types (
    card_id VARCHAR(20),
    type VARCHAR(50),
    FOREIGN KEY (card_id) REFERENCES cards_classification(id),
    PRIMARY KEY (card_id, type)
);

CREATE TABLE cards_gameplay (
    card_id VARCHAR(20) PRIMARY KEY,
    hp INT,
    evolves_from VARCHAR(255),
    abilities JSON,
    attacks JSON,
    weaknesses JSON,
    retreat_cost JSON,
    converted_retreat_cost INT,
    legality JSON,
    FOREIGN KEY (card_id) REFERENCES cards_classification(id)
);

CREATE TABLE cards_lore (
    card_id VARCHAR(20) PRIMARY KEY,
    level VARCHAR(50),
    flavor_text TEXT,
    national_pokedex_numbers JSON,
    FOREIGN KEY (card_id) REFERENCES cards_classification(id)
);

CREATE TABLE cards_art (
    card_id VARCHAR(20) PRIMARY KEY,
    artist VARCHAR(255),
    small_image VARCHAR(500),
    large_image VARCHAR(500),
    FOREIGN KEY (card_id) REFERENCES cards_classification(id)
);

CREATE TABLE decks (
    id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    types JSON
);

CREATE TABLE deck_cards (
    deck_id VARCHAR(20),
    card_id VARCHAR(20),
    count INT,
    FOREIGN KEY (deck_id) REFERENCES decks(id),
    FOREIGN KEY (card_id) REFERENCES cards_classification(id),
    PRIMARY KEY (deck_id, card_id)
);

CREATE TABLE preprocessed_images (
    id INT AUTO_INCREMENT PRIMARY KEY,
    card_id VARCHAR(20),
    average_color_rgb JSON,
    average_color_hsv JSON,
    average_color_lab JSON,
    median_color_rgb JSON,
    median_color_hsv JSON,
    median_color_lab JSON,
    FOREIGN KEY (card_id) REFERENCES cards_classification(id)
);

-- ─────────────────────────────────────────────────────────────────────────────
-- Card identification & pricing tables (added for webcam scanner feature)
-- ─────────────────────────────────────────────────────────────────────────────

-- Stores perceptual-hash fingerprints used for fast visual card matching.
-- phash_int is the 64-bit integer representation of a pHash computed from the
-- card's small_image; phash_str keeps the hex form for debugging.
CREATE TABLE card_match_index (
    card_id    VARCHAR(20) PRIMARY KEY,
    phash_int  BIGINT UNSIGNED NOT NULL,
    phash_str  VARCHAR(16) NOT NULL,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (card_id) REFERENCES cards_classification(id)
);

-- Stores market price snapshots fetched from external sources (e.g. pokemontcg.io
-- which proxies TCGplayer / Cardmarket data).
CREATE TABLE card_prices (
    id           INT AUTO_INCREMENT PRIMARY KEY,
    card_id      VARCHAR(20) NOT NULL,
    source       VARCHAR(50) NOT NULL COMMENT 'e.g. tcgplayer, cardmarket',
    market       VARCHAR(50) NOT NULL COMMENT 'e.g. normal, holofoil, 1stEditionHolofoil',
    currency     CHAR(3)     NOT NULL DEFAULT 'USD',
    condition    VARCHAR(30)          COMMENT 'e.g. near_mint, lightly_played, market',
    price        DECIMAL(10,2),
    captured_at  DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (card_id) REFERENCES cards_classification(id),
    INDEX idx_card_prices_card_id (card_id),
    INDEX idx_card_prices_captured_at (captured_at)
);

-- Optional history of scans performed via the webcam UI.
CREATE TABLE scan_history (
    id               INT AUTO_INCREMENT PRIMARY KEY,
    scanned_at       DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    matched_card_id  VARCHAR(20),
    confidence       FLOAT,
    hamming_distance INT          COMMENT 'Lower = more similar',
    source_device    VARCHAR(255) COMMENT 'User-Agent of the scanning client',
    FOREIGN KEY (matched_card_id) REFERENCES cards_classification(id),
    INDEX idx_scan_history_card_id (matched_card_id)
);
