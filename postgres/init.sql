--
-- PostgreSQL database dump
--

\restrict Q6yAPPEYXa7FnGOiNbkZx7F1t4mbia2ce8f27ANoCwp0yQkVYvgfEUQKExuvnhh

-- Dumped from database version 16.15
-- Dumped by pg_dump version 16.15

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: scoring_results; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.scoring_results (
    id bigint NOT NULL,
    transaction_id text NOT NULL,
    score double precision NOT NULL,
    fraud_flag integer NOT NULL,
    model_version text NOT NULL,
    us_state text,
    merch text,
    cat_id text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT scoring_results_fraud_flag_check CHECK ((fraud_flag = ANY (ARRAY[0, 1]))),
    CONSTRAINT scoring_results_score_check CHECK (((score >= (0)::double precision) AND (score <= (1)::double precision)))
);


--
-- Name: scoring_results_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.scoring_results_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: scoring_results_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.scoring_results_id_seq OWNED BY public.scoring_results.id;


--
-- Name: scoring_results id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scoring_results ALTER COLUMN id SET DEFAULT nextval('public.scoring_results_id_seq'::regclass);


--
-- Name: scoring_results scoring_results_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scoring_results
    ADD CONSTRAINT scoring_results_pkey PRIMARY KEY (id);


--
-- Name: scoring_results scoring_results_transaction_id_model_version_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scoring_results
    ADD CONSTRAINT scoring_results_transaction_id_model_version_key UNIQUE (transaction_id, model_version);


--
-- Name: idx_scoring_results_created_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_scoring_results_created_at ON public.scoring_results USING btree (created_at DESC);


--
-- Name: idx_scoring_results_model_version; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_scoring_results_model_version ON public.scoring_results USING btree (model_version);


--
-- PostgreSQL database dump complete
--

\unrestrict Q6yAPPEYXa7FnGOiNbkZx7F1t4mbia2ce8f27ANoCwp0yQkVYvgfEUQKExuvnhh

