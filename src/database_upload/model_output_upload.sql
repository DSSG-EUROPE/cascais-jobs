drop schema if exists model_output cascade;
create schema model_output;

--grant privileges
GRANT ALL ON SCHEMA model_output TO dssg2017_admin;
grant all on schema model_output to hholmestad, tfilho, kyang;

--create tables
--model configurations
CREATE TABLE model_output.model_configs (
        model_id VARCHAR(100) NOT NULL,
        model_type VARCHAR(35) NOT NULL,
        params VARCHAR(300),
        feature_set VARCHAR(100) NOT null,
        experiment VARCHAR(100) NOT NULL
);

--train_test configurations
CREATE TABLE model_output.train_test_configs (
        model_id VARCHAR(100) NOT NULL,
        train_test_id VARCHAR(100) NOT null,
        split_type VARCHAR(35) not null,
        action_date DATE NOT NULL,
        train_timedelta VARCHAR(35),
        train_total INTEGER NOT NULL,
        train_proportion_ltu numeric(7,6) NOT NULL,
        test_total INTEGER NOT NULL,
        test_proportion_ltu numeric(7,6) NOT NULL
);


--model performances
CREATE TABLE model_output.performances (
        model_id VARCHAR(100) NOT NULL,
        train_test_id VARCHAR(100) NOT null,
        k_value INTEGER not null,
        k_type VARCHAR(16) NOT NULL,
        metric VARCHAR(16) NOT NULL,
        subset_type VARCHAR(30) not null,
        subset_cutoff numeric not null,
        month_threshold numeric not null,
        subset_size integer not null,
        subset_ltu_prop numeric not null,
        value numeric(7,6) not null
);

--feature importances
CREATE TABLE model_output.feature_importances (
        model_id VARCHAR(100) NOT NULL,
        train_test_id VARCHAR(100) NOT null,
        feature VARCHAR(100) not null,
        importance numeric(7,6) NOT NULL
);
