SCHEMA_SQL = """
-- Enable pgvector extension for embeddings
CREATE EXTENSION IF NOT EXISTS vector;

-- Processing queue table
CREATE TABLE IF NOT EXISTS processing_queue (
    id SERIAL PRIMARY KEY,
    batch_id VARCHAR(100) UNIQUE NOT NULL,
    firm_name VARCHAR(100) NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',
    total_reviews INTEGER,
    processed_reviews INTEGER DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Topics table
CREATE TABLE IF NOT EXISTS topics (
    id SERIAL PRIMARY KEY,
    firm_name VARCHAR(100) NOT NULL,
    topic_name VARCHAR(200),
    keywords JSONB,
    representative_docs JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Main reviews table with enriched data
CREATE TABLE IF NOT EXISTS reviews (
    id SERIAL PRIMARY KEY,
    firm_name VARCHAR(100) NOT NULL,
    review_id VARCHAR(100) UNIQUE NOT NULL,
    author_name VARCHAR(200),
    rating INTEGER CHECK (rating >= 1 AND rating <= 5),
    date_posted DATE,
    content TEXT NOT NULL,
    title VARCHAR(500),
    
    -- Validation metadata
    is_valid BOOLEAN DEFAULT true,
    validation_flags JSONB DEFAULT '{}',
    language VARCHAR(10),
    
    -- NLP enrichments
    sentiment_score FLOAT,
    sentiment_label VARCHAR(20),
    emotion_scores JSONB,
    summary TEXT,
    
    -- Extracted entities and aspects
    entities JSONB DEFAULT '[]',
    aspects JSONB DEFAULT '[]',
    key_phrases JSONB DEFAULT '[]',
    
    -- Topic modeling
    primary_topic_id INTEGER,
    topic_distribution JSONB,
    
    -- Embeddings stored as array
    embedding VECTOR(384),  -- Using pgvector extension
    
    -- Metadata
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processing_version VARCHAR(20) DEFAULT '1.0'
);

-- Create indexes for performance
CREATE INDEX idx_reviews_firm ON reviews(firm_name);
CREATE INDEX idx_reviews_sentiment ON reviews(sentiment_score);
CREATE INDEX idx_reviews_date ON reviews(date_posted);
CREATE INDEX idx_reviews_topic ON reviews(primary_topic_id);
""" 