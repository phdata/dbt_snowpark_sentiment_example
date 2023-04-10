
USE SCHEMA [SCHEMA_TO_USE];

CREATE STAGE IF NOT EXISTS DBT_DEPS;

PUT file://[path_to_project]/jaffle_shop/vader_lexicon.zip
    @DBT_DEPS/nltk_data/sentiment/
    auto_compress = false
    overwrite = true;

