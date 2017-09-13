--drop schema if you want to start over **WILL DELETE _OLD TABLES
drop schema if exists iefp cascade;
create schema iefp;

--grant privileges
GRANT ALL ON SCHEMA iefp TO dssg2017_admin;
grant all on schema iefp to hholmestad, tfilho, kyang;

--create tables
--new version of applications table (sie-11 / Pedido)
CREATE TABLE iefp.application (
        anomes INTEGER NOT NULL,
        mo_data_movimento VARCHAR(19) NOT NULL,
        cfreguesia INTEGER NOT NULL,
        dfreguesia VARCHAR(27) NOT NULL,
        ctipo_movimento INTEGER NOT NULL,
        dtipo_movimento VARCHAR(35) NOT NULL,
        ute_id INTEGER NOT NULL,
        cnp_pretendida VARCHAR(8),
        dcnp_pretendida VARCHAR(65),
        cpp_pretendida VARCHAR(7),
        dcpp_pretendida VARCHAR(145),
        candidatura_data VARCHAR(19) NOT NULL,
        cnacionalidade VARCHAR(2) NOT NULL,
        dnacionalidade VARCHAR(32) NOT NULL,
        sexo VARCHAR(1) NOT NULL,
        chabilitacao_escolar VARCHAR(2) NOT NULL,
        dhabilitacao_escolar VARCHAR(26) NOT NULL,
        cdeficiencia INTEGER NOT NULL,
        ddeficiencia VARCHAR(58) NOT NULL,
        ccategoria INTEGER NOT NULL,
        dcategoria VARCHAR(25) NOT NULL,
        candidatura_motivo_inscricao INTEGER NOT NULL,
        candidatura_rinsc VARCHAR(1) NOT NULL,
        ute_estado_civil VARCHAR(1) NOT NULL,
        dmotivo_inscricao VARCHAR(86) NOT NULL,
        candidatura_int_tempo_nao_insc VARCHAR(5),
        candidatura_prof_pret_tempo_pratica VARCHAR(5),
        sit_anterior_prof_tempo_pratica VARCHAR(5),
        candidatura_local_trabalho VARCHAR(4),
        ute_plano_emprego VARCHAR(1) NOT NULL,
        ute_plano_emprego_anterior VARCHAR(1) NOT NULL,
        candidatura_carteira_prof VARCHAR(4),
        candidatura_formacao_prof VARCHAR(4),
        candidatura_qualificacao VARCHAR(4),
        reinscricao_ult_saida_data VARCHAR(19),
        reinscricao_ult_saida_motivo VARCHAR(4),
        carea_formacao_tabela_em_activo VARCHAR(5),
        darea_formacao_tabela_em_activo VARCHAR(86),
        carea_curso_tabela_em_activo VARCHAR(5),
        darea_curso_tabela_em_activo VARCHAR(95),
        colocacao_regime_contrato VARCHAR(4),
        colocacao_tipo_contrato VARCHAR(4),
        cnp_anterior VARCHAR(8),
        dcnp_anterior VARCHAR(65),
        cpp_anterior VARCHAR(8),
        dcpp_anterior VARCHAR(145),
        dregime_contrato_pretendido VARCHAR(8) NOT NULL,
        sub_rsi VARCHAR(1) NOT NULL,
        ute_curso_estabelec_ens_superior VARCHAR(6),
        ute_curso_ano_conclusao VARCHAR(6),
        candidatura_origem VARCHAR(5),
        cae_anterior VARCHAR(7),
        dcae_anterior VARCHAR(167),
        ute_data_nascimento VARCHAR(19),
        conjuge_estado_civil VARCHAR(4),
        conjuge_categoria VARCHAR(4),
        conjuge_estado VARCHAR(4),
        conjuge_motivo_indisponibilidade VARCHAR(4),
        candidatura_categoria_anterior VARCHAR(4),
        candidatura_estado_anterior VARCHAR(4),
        ute_nr_pessoas_cargo VARCHAR(4),
        ute_nr_descendentes_cargo VARCHAR(4),
        candidatura_data_ppe VARCHAR(19),
        ute_idade INTEGER NOT NULL,
        intervencoes INTEGER NOT NULL,
        sub_data_inicio VARCHAR(19),
        sub_data_fim VARCHAR(19),
        sub_data_extincao VARCHAR(19),
        sub_data_suspensao VARCHAR(19)
);

--cancellation (unl-sie-31) - same as old table
CREATE TABLE iefp.cancellation (
		--table_index SERIAL primary key,
        anomes INTEGER NOT NULL,
        ctipo_movimento INTEGER NOT NULL,
        dtipo_movimento VARCHAR(28) NOT NULL,
        ute_id INTEGER NOT NULL,
        anulacao_motivo INTEGER NOT NULL,
        dmotivo_anulacao VARCHAR(89) NOT NULL,
        anulacao_data VARCHAR(19)
);

--intervention (unl-sie-35-novo envio_v2)
CREATE TABLE iefp.intervention (
		--table_index SERIAL primary key,
        anomes INTEGER NOT NULL,
        ctipo_movimento INTEGER NOT NULL,
        dtipo_movimento VARCHAR(24) NOT NULL,
        mo_data_movimento VARCHAR(19) NOT NULL,
        intervencao_data VARCHAR(19),
        ute_id INTEGER NOT NULL,
        intervencao_codigo INTEGER NOT NULL,
        intervencao_codigo_d VARCHAR(89) NOT NULL,
        intervencao_ind VARCHAR(4),
        intervencao_ind_d VARCHAR(45),
        intervencao_resultado INTEGER NOT NULL,
        dresultado_intervencao VARCHAR(74)
);

--intervention_referrals (unl-sie-36_v2-Move36.csv)
CREATE TABLE iefp.intervention_referrals(
        --table_index SERIAL primary key,
		anomes INTEGER NOT NULL,
        ctipo_movimento INTEGER NOT NULL,
        dtipo_movimento VARCHAR(25) NOT NULL,
        mo_data_movimento VARCHAR(19) NOT NULL,
        intervencao_data VARCHAR(19) NOT NULL,
        ute_id INTEGER NOT NULL,
        intervencao_codigo INTEGER NOT NULL,
        intervencao_codigo_d VARCHAR(78) NOT NULL,
        intervencao_resultado INTEGER NOT NULL,
        dresultado_intervencao VARCHAR(31)
);

--interview (unl-sie-37-novo envio_v2)
CREATE TABLE iefp.interview(
        --table_index SERIAL primary key,
		anomes INTEGER NOT NULL,
        ctipo_movimento INTEGER NOT NULL,
        dtipo_movimento VARCHAR(25) NOT NULL,
        mo_data_movimento VARCHAR(19) NOT NULL,
        apresentacao_data VARCHAR(19) NOT NULL,
        ute_id INTEGER NOT NULL,
        oferta_nr INTEGER NOT NULL,
        oferta_servico INTEGER NOT NULL,
        apresentacao_resultado INTEGER NOT NULL,
        dresultado_apresentacao VARCHAR(89) NOT NULL
);

--convocation (unl-sie-38_v2)
CREATE TABLE iefp.convocation(
        --table_index SERIAL primary key,
		anomes INTEGER NOT NULL,
        ctipo_movimento INTEGER NOT NULL,
        dtipo_movimento VARCHAR(24) NOT NULL,
        mo_data_movimento VARCHAR(19) NOT NULL,
        convocatoria_em VARCHAR(19) NOT NULL,
        ute_id INTEGER NOT NULL,
        convocatoria_tipo VARCHAR(2) NOT NULL,
        dtipo_convocatoria VARCHAR(70) NOT NULL,
        convocatoria_resultado INTEGER NOT NULL,
        dresultado_convocatoria VARCHAR(55) NOT NULL
);

--category_change (unl-sie-43_v2)
CREATE TABLE iefp.category_change(
        --table_index SERIAL primary key,
		anomes INTEGER NOT NULL,
        ctipo_movimento INTEGER NOT NULL,
        dtipo_movimento VARCHAR(19) NOT NULL,
        mo_data_movimento VARCHAR(19) NOT NULL,
        ute_id INTEGER NOT NULL,
        ccategoria INTEGER NOT NULL,
        dcategoria VARCHAR(39) NOT NULL,
        candidatura_categoria_anterior VARCHAR(4),
        dcategoria_anterior VARCHAR(39)
);

CREATE TABLE iefp.movement (
		ute_id INTEGER,
		movement_event_date VARCHAR(19),
		application_id INTEGER,
		movement_type VARCHAR(100),
		movement_subtype VARCHAR(100),
		movement_result VARCHAR(100),
		movement_index INTEGER,
		PRIMARY KEY (ute_id,movement_type,movement_index)
);
