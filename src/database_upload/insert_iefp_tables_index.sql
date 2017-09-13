--add table index
ALTER TABLE iefp.application ADD COLUMN table_index SERIAL PRIMARY KEY;
ALTER TABLE iefp.cancellation ADD COLUMN table_index SERIAL PRIMARY KEY;
ALTER TABLE iefp.category_change ADD COLUMN table_index SERIAL PRIMARY KEY;
ALTER TABLE iefp.convocation ADD COLUMN table_index SERIAL PRIMARY KEY;
ALTER TABLE iefp.intervention ADD COLUMN table_index SERIAL PRIMARY KEY;
ALTER TABLE iefp.intervention_referrals ADD COLUMN table_index SERIAL PRIMARY KEY;
ALTER TABLE iefp.interview ADD COLUMN table_index SERIAL PRIMARY KEY;
