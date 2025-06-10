DROP TRIGGER IF EXISTS prevent_dup_mart ON data_mart.client_segments;

CREATE OR REPLACE FUNCTION data_mart.check_duplicate()
RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM data_mart.client_segments
        WHERE client_id = NEW.client_id
          AND client_name = NEW.client_name
          AND client_revenue = NEW.client_revenue
          AND usage_count = NEW.usage_count
          AND client_segment = NEW.client_segment
          AND last_usage_date = NEW.last_usage_date
    ) THEN
        RETURN NULL; 
    END IF;

    RETURN NEW; 
END;
$$ LANGUAGE plpgsql;


create trigger prevent_dup_mart
before insert on data_mart.client_segments
for each row
execute function data_mart.check_duplicate();