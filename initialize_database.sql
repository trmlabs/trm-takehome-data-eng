CREATE DATABASE trm_db;

CREATE TABLE public.daily_transfers
(
    sender text NOT NULL,
    receiver text NOT NULL,
    total_value double precision NOT NULL,
    date date NOT NULL,
    PRIMARY KEY (sender, receiver, date)
);

CREATE INDEX sender_index
    ON public.daily_transfers USING btree
    (sender ASC NULLS LAST, date ASC NULLS LAST)
;

CREATE INDEX receiver_index
    ON public.daily_transfers USING btree
    (receiver ASC NULLS LAST, date ASC NULLS LAST)
;
