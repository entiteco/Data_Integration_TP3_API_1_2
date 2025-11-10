USE tp3;

-- -------------------------------------------------------------------
-- Montant dépensé par utilisateur et type de transaction
-- -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS total_depense_par_user_type (
    user_id VARCHAR(255) NOT NULL,
    transaction_type VARCHAR(255) NOT NULL,
    total_depense_usd DECIMAL(15, 2),
    last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, transaction_type)
);


-- -------------------------------------------------------------------
-- Cycle de vie (statuts) d'une transaction
-- -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS transaction_lifecycle (
    transaction_id VARCHAR(255) NOT NULL,
    status_history JSON,
    last_update_time TIMESTAMP,
    last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (transaction_id)
);


-- -------------------------------------------------------------------
-- Total par type sur une fenêtre glissante de 5min
-- -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS total_par_type_5min_sliding (
    window_start_time TIMESTAMP NOT NULL,
    transaction_type VARCHAR(255) NOT NULL,
    total_amount_5min DECIMAL(15, 2),
    last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (window_start_time, transaction_type)
);


-- -------------------------------------------------------------------
-- Modélisation et création de la table user_permissions dans MySQL.
-- -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS user_permissions (
    user_id VARCHAR(255) NOT NULL,
    folder_path VARCHAR(512) NOT NULL,
    permission ENUM('read', 'write', 'none') DEFAULT 'none',
    PRIMARY KEY (user_id, folder_path)
);


-- -------------------------------------------------------------------
-- Table pour la Q2: Top 3 produits les plus achetés
-- -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS product_purchase_counts (
    product_id VARCHAR(255) NOT NULL,
    purchase_count BIGINT DEFAULT 0,
    last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (product_id)
);