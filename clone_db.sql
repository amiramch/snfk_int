USE ROLE ACCOUNTADMIN;
USE WAREHOUSE ANALYSIS_WH;
CREATE OR REPLACE DATABASE SPOTIFY_CLONED;
CREATE SCHEMA DEMO;

CREATE TABLE SPOTIFY_CLONED CLONE EXTERNAL_ACCESS_DEMO.DEMO.EPISODES_FINAL;
