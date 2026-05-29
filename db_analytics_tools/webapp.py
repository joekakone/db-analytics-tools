# coding : utf-8


"""
    DB Analytics Tools : Web User Interface

    This module provides classes and functions for database interactions and data migration.
"""


import os
import json
import time
import datetime
import argparse
import subprocess
import tempfile

import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from psycopg2 import OperationalError
from sqlalchemy import text

import db_analytics_tools as db
import db_analytics_tools.integration as dbi
from db_analytics_tools.airflow import AirflowRESTAPI

from db_analytics_tools.scheduler import CronManager



#####################################################################################################################################
# DB Analytics UI Class
#####################################################################################################################################
class DBAnalyticsUI:
    """
    A Streamlit-based web application for managing database analytics tools, including database connections,
    """
    app_title = "DB Analytics Tools UI"
    app_favicon = "https://joekakone.github.io/db-analytics-tools/static/img/favicon.svg"
    app_description = """A unified DataOps & ETL Orchestration Platform powered by DB Analytics Tools. 
    Streamline your data engineering workflows with an all-in-one intuitive console designed to manage database environments, 
    automate cron scheduling via db_cli, control Airflow DAGs, and seamlessly bridge your infrastructure to enterprise reporting."""
    #################################################################################################################################
    # Initialization & State Management
    #################################################################################################################################
    def __init__(self, config):
        """
        Initializes the UI with database configuration and pipelines.

        :param config: A dictionary containing configuration details for the database,
                       allowed users, and pipeline configurations.
        """
        self.config = config
        self.app_name = config.get("app_name", "DB Analytics Tools UI")
        self.app_logo = config.get("app_logo", "")
        self.allowed_users = config.get("allowed_users", [])
        
        #############################################################################################################################
        # Initialize session state variables if they don't exist
        #############################################################################################################################
        initial_state = {
            "auth_status": False,
            "user": None,
            "current_client": None,
            "current_server_name": None,
            "active_module": None,
            "current_airflow": None
        }
        
        for key, value in initial_state.items():
            if key not in st.session_state:
                st.session_state[key] = value
    #################################################################################################################################


    #################################################################################################################################
    # Main Application Logic
    #################################################################################################################################
    def run(self):
        """
        Launch the Streamlit application.
        """
        st.set_page_config(page_title=self.app_title, page_icon=self.app_favicon, layout="wide")
        
        #############################################################################################################################
        # 1. Authentication
        #############################################################################################################################
        if not self.check_auth() or not st.session_state.auth_status:
            return

        #############################################################################################################################
        # 2. Sidebar Management
        #############################################################################################################################
        self.sidebar_management()

        #############################################################################################################################
        # 3. Route
        #############################################################################################################################
        module = st.session_state.active_module

        if module == "DB":
            self._render_db_module()
        elif module == "Airflow":
            self._render_airflow_module()
        else:
            self._render_default_view()
    #################################################################################################################################


    #################################################################################################################################
    # Authentication
    #################################################################################################################################
    def check_auth(self):
        """
        Gère l'écran de connexion avec authentification par question secrète.
        """
        if not st.session_state.auth_status:
            col1, col2, col3 = st.columns([1, 2, 1])
            with col2:
                st.markdown(f'<div style="text-align: center;"><img src="{self.app_logo}" width="150"></div>', unsafe_allow_html=True)
                st.markdown('<h1 style="text-align: center;">Authentication 🔒</h1>', unsafe_allow_html=True)
                
                # Credentials Form
                user = st.text_input("Username")
                question = self.config.get("auth_config", {}).get("secret_question", "Secret Question")
                answer = st.text_input(f"Secret Question : {question}", type="password")
                accept = st.checkbox("I accept the terms and conditions", value=False)
                
                # Authentication Logic
                if st.button("Login", width='stretch'):
                    expected_answer = self.config.get("auth_config", {}).get("secret_answer", "")
                    if user == "" or answer == "":
                        st.error("Missing credentials!", width='stretch')
                    elif not (user in self.allowed_users or self.allowed_users == []):
                        st.error("You are not allowed ! Please contact the administrator.", width='stretch')
                    elif not accept:
                        st.error("You must accept the terms and conditions to get access to the application.", width='stretch')
                    elif answer.lower() == expected_answer.lower() and (user in self.allowed_users or self.allowed_users == []):
                        st.session_state.auth_status = True
                        st.session_state.user = user
                        st.success("Authentication Succeed ! Redirection...", width='stretch')
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error("Something went wrong !", width='stretch')
            return False
        return True
    #################################################################################################################################


    #################################################################################################################################
    # Sidebar Management
    #################################################################################################################################
    def sidebar_management(self):
        """
        Sidebar : Server selection, Connection DB and Airflow.
        """
        with st.sidebar:
            #########################################################################################################################
            # Branding & User Info
            #########################################################################################################################
            st.markdown(f'<h2 style="text-align: center;">DB Analytics Tools</h2>', unsafe_allow_html=True)
            st.markdown(f'<p style="text-align: center;">Speed up your analytics workflows</p>', unsafe_allow_html=True)
            if self.app_logo:
                st.markdown(f'<div style="text-align: center;"><img src="{self.app_logo}" width="150"></div>', unsafe_allow_html=True)
            st.markdown(f'<p style="text-align: center;">Hello <strong>{st.session_state.user}</strong></p>', unsafe_allow_html=True)
            #########################################################################################################################
            
            
            #########################################################################################################################
            # Navigation
            #########################################################################################################################
            is_at_home = st.session_state.active_module is None
            if not is_at_home:
                st.markdown("---")
                if st.button("🏠 Back to Home", type="tertiary", width='stretch'):
                    st.session_state.active_module = None
                    st.rerun()
            st.markdown("---")
            #########################################################################################################################
            
            
            #########################################################################################################################
            # Databases
            #########################################################################################################################
            self._render_db_section()
            
            
            #########################################################################################################################
            # Airflow
            #########################################################################################################################
            self._render_airflow_section()
            
            
            #########################################################################################################################
            # Power BI
            #########################################################################################################################
            self._render_external_links()
            
            
            #########################################################################################################################
            # Logout Button
            #########################################################################################################################
            if st.button("Logout", type="primary", width='stretch'):
                st.session_state.auth_status = False
                st.session_state.user = None
                st.session_state.current_client = None
                st.session_state.current_server_name = None
                st.session_state.active_module = None
                st.session_state.current_airflow = None
                st.rerun()
            #########################################################################################################################
            
            
            #########################################################################################################################
            # Footer
            #########################################################################################################################
            st.sidebar.markdown("---")
            st.sidebar.markdown(
                "© 2023 DB Analytics Tools | [About](https://pypi.org/project/db-analytics-tools/)",
                unsafe_allow_html=True
            )
            #########################################################################################################################
    #################################################################################################################################


    #################################################################################################################################
    # Defaut Content
    #################################################################################################################################
    def _render_default_view(self):
        """
        Render the default view.
        """
        st.title(self.app_name)
        st.info(self.app_description)
        
        
        #############################################################################################################################
        # Cron Jobs Management
        #############################################################################################################################
        cron_manager = CronManager()
        try:
            cron_jobs = cron_manager.read()
        except Exception as e:
            pass

        st.markdown("---")
        st.subheader("Scheduled Jobs ⏰")
        if cron_jobs.empty:
            st.warning("No scheduled jobs found.", width='stretch')
            # return
        else:
            print(cron_jobs)
            print(len(cron_jobs))
            st.dataframe(cron_jobs, width='stretch', hide_index=True)
        
        tabs = st.tabs([
            "Manage Jobs 🛠️",
            "Schedule a job 📅", 
        ])
        
        with tabs[0]:
            if cron_jobs.empty:
                st.warning("No scheduled jobs found.", width='stretch')
                # return
            else:
                try:
                    click_on_preview_job = False
                    selected_job = st.selectbox("Select a job for action", cron_jobs['id'].unique())
                    selected_job = selected_job.replace("DB_TOOLS_ID:", "")
                    selected_job_df = cron_jobs[cron_jobs['id'] == selected_job]
                    selected_job_command = selected_job_df.iloc[0]['command']
                    selected_job_schedule_interval = selected_job_df.iloc[0]['schedule_interval']
                    is_disabled = selected_job_df.iloc[0]['raw'].startswith("#")
                    
                    c1, c2, c3 = st.columns(3)
                    
                    with c1:
                        with st.form("reschedule_job", clear_on_submit=False, border=False):
                            schedule_interval = st.text_input("Reschedule Job", placeholder="e.g. 0 0 * * *")
                            if st.form_submit_button("📝 Reschedule", width='stretch'):
                                cron_manager.update(
                                    comment=selected_job,
                                    new_cmd=selected_job_command,
                                    new_schedule=schedule_interval
                                )
                                st.success(f"Job rescheduled to {schedule_interval}.")

                    with c2:
                        with st.form("update_command", clear_on_submit=False, border=False):
                            job_command = st.text_input("Update Job Command", placeholder="e.g. db_cli --engine greenplum --host localhost --port 5432 --database cdrfw --user joekakone --password mypassword --start 3 --stop 1 --freq m --functions prod.fn_preprocess_sales prod.fn_agregate_sales")
                            if st.form_submit_button("📝 Update Command", width='stretch'):
                                cron_manager.update(
                                    comment=selected_job,
                                    new_cmd=job_command,
                                    new_schedule=selected_job_schedule_interval
                                )
                                st.success(f"Job command updated to {job_command}.")

                    with c3:
                        st.markdown('<div style="margin-bottom: 0.15rem;"><span style="font-size: 14px; margin-bottom: 0rem;">⚠️ Use with caution !</span></div>', unsafe_allow_html=True)
                        if is_disabled:
                            if st.button("✅ Enable Job", width='stretch'):
                                cron_manager.enable(comment=selected_job)
                                st.success(f"Job {selected_job} enabled.")
                        else:
                            if st.button("🗑️ Disable Job", type="primary", width='stretch'):
                                cron_manager.disable(comment=selected_job)
                                st.success(f"Job {selected_job} disabled.")

                        if st.button("🗑️ Delete Job", type="primary", width='stretch'):
                            cron_manager.delete(comment=selected_job)
                            st.error(f"Job {selected_job} deleted.")
                    
                    if click_on_preview_job:
                        st.dataframe(selected_job_df, width='stretch', hide_index=True)
                        click_on_preview_job = False

                except Exception as e:
                    st.error(f"Erreur d'accès aux tables : {e}")

        
        with tabs[1]:
            with st.form("schedule_job_form", clear_on_submit=True, border=False):
                job_command = st.text_input("Command to Schedule", placeholder="e.g. db_cli --engine greenplum --host XX.XXX.XX.XXX --port 5432 --database cdrfw --user joekakone --password Axian2580 --start 2026-01-01 --stop 2026-05-01 --functions bibox.fn_gros_ad_lu_agents bibox.fn_gros_ad_lu_agents_month_alignement --frequency m")
                job_schedule = st.text_input("Cron Schedule", placeholder="e.g. 0 0 * * *")
                job_comment = st.text_input("Unique Job Comment (ID)", placeholder="e.g. db_sync_weekly")
                submit_job = st.form_submit_button("Schedule Job", type="primary", width='stretch')
                
                if submit_job:
                    if not job_command or not job_schedule or not job_comment:
                        st.error("Please fill in all fields to schedule a job.")
                    else:
                        success = cron_manager.create(job_command, job_schedule, comment=job_comment)
                        if success:
                            st.success(f"Job '{job_comment}' scheduled successfully!")
                        else:
                            st.error(f"A job with the comment '{job_comment}' already exists.")            
    #################################################################################################################################


    #################################################################################################################################
    # Databases
    #################################################################################################################################
    def _render_db_module(self):
        """
        Render the Database module
        """
        if not st.session_state.current_client:
            st.warning("⚠️ No connected DB client. Please select a server in the sidebar.")
            return
        
        tabs = st.tabs([
            "🏠 DB Summary", 
            "⚡ ETL Execution", 
            "🕵️ Sessions & Locks", 
            "📂 Tables & Storage", 
            "📥 Upload & Download", 
            "🎮 Query Console"
        ])
        
        with tabs[0]: self.db_page_summary()
        with tabs[1]: self.db_page_execution()
        with tabs[2]: self.db_page_sessions()
        with tabs[3]: self.db_page_tables()
        with tabs[4]: self.db_page_upload_download()
        with tabs[5]: self.db_page_query_console()
    #################################################################################################################################


    #################################################################################################################################
    # Airflow
    #################################################################################################################################
    def _render_airflow_module(self):
        """
        Render the Airflow module.
        """
        if not st.session_state.current_airflow:
            st.warning("⚠️ No configured Airflow Instance.")
            return

        tabs = st.tabs([
            "🏠 DAG Summary", 
            "⚡ DAG Execution"
        
        ])
        with tabs[0]: self.airflow_page_summary()
        with tabs[1]: self.airflow_page_execution()
    #################################################################################################################################







    #################################################################################################################################
    # Databases
    #################################################################################################################################
    def _render_db_section(self):
        """
        Render the Databases section in the sidebar.
        """
        database_list = self.config.get("database_instances", []) + [{"name": "Custom Database"}]
        if not database_list:
            return
        
        is_connected = st.session_state.current_client is not None
        status_emoji = "🟢" if is_connected else "🔴"
        st.subheader(f"🛢️ Databases {status_emoji}")
        
        database_names = [s["name"] for s in database_list]
        selected_database_name = st.selectbox("Choose a DB Server", database_names)
        database_cfg = next(s for s in database_list if s["name"] == selected_database_name)
        
        # DB Authentication Form
        with st.expander("DB Authentication 🔑", expanded=(st.session_state.current_client is None)):
            db_engine = st.text_input("Engine", value=database_cfg.get("engine"), placeholder="e.g. postgres, sqlserver")
            db_host = st.text_input("Host", value=database_cfg.get("host"))
            db_port = st.text_input("Port", value=database_cfg.get("port"))
            db_database = st.text_input("Database", value=database_cfg.get("database"))
            db_user = st.text_input("Username")
            db_pass = st.text_input("Password", type="password")
            
            if st.button("Connect to DB", type="primary", width='stretch'):
                try:
                    # Setup client
                    client = db.Client(
                        host=db_host,
                        port=db_port,
                        database=db_database,
                        username=db_user,
                        password=db_pass,
                        engine=db_engine
                    )
                    st.session_state.pipelines = self.get_pipelines(database_cfg.get("pipelines", []))
                    
                    st.session_state.current_client = client
                    st.session_state.etl = dbi.ETL(st.session_state.current_client)
                    
                    st.session_state.current_server_name = selected_database_name
                    st.success(f"Connected to {selected_database_name}")
                    
                    # Active Module
                    st.session_state.active_module = "DB"
                    
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed to connect : {e}")
                    print(e)

        st.markdown("---")
    #################################################################################################################################


    #################################################################################################################################
    # Airflow
    #################################################################################################################################
    def _render_airflow_section(self):
        """
        Render the Airflow section in the sidebar.
        """
        airflow_list = self.config.get("airflow_instances", [])
        if not airflow_list:
            return
        
        is_connected = st.session_state.current_airflow is not None
        status_emoji = "🟢" if is_connected else "🔴"
        st.subheader(f"🏗️ Airflow {status_emoji}")
        
        airflow_list_names = [a["name"] for a in airflow_list]
        selected_airflow_name = st.selectbox("Choose an Airflow Instance", airflow_list_names)
        airflow_cfg = next(a for a in airflow_list if a["name"] == selected_airflow_name)
        
        # Airflow Authentication Form
        with st.expander("Airflow Authentication 🔑", expanded=(st.session_state.current_client is None)):
            airflow_user = st.text_input("Airflow Username")
            airflow_pass = st.text_input("Airflow Password", type="password")
            
            if st.button("Connect to Airflow", width='stretch'):
                try:
                    # Setup client
                    airflow = AirflowRESTAPI(
                        base_url=airflow_cfg["url"],
                        api_endpoint=airflow_cfg["api_endpoint"],
                        username=airflow_user,
                        password=airflow_pass,
                    )
                    st.session_state.current_airflow = airflow
                    st.session_state.current_airflow_name = selected_airflow_name
                    st.success(f"Connected to {selected_airflow_name}")
                    
                    # Active Module
                    st.session_state.active_module = "Airflow"
                    
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed to connect : {e}")
                    print(e)

        st.markdown("---")
    #################################################################################################################################


    #################################################################################################################################
    # Power BI
    #################################################################################################################################
    def _render_external_links(self):
        """
        Renders external links for Power BI instances.
        """
        powerbi_list = self.config.get("powerbi_instances", [])
        
        if powerbi_list:
            st.subheader("📊 Power BI")
            for pb in powerbi_list:
                st.link_button(f"{pb['name']}", pb['url'], width='stretch')
        
        st.markdown("---")
    #################################################################################################################################


    #################################################################################################################################
    # Authentication & Sidebar Management
    #################################################################################################################################
    @staticmethod
    def get_pipelines(input_pipelines):
        """
        Processes and organizes pipeline configurations into a dictionary format
        with pipeline names as keys and pipeline details as values. Additionally,
        it adds single function options for multiple pipelines.

        :param pipelines: A list of pipeline configuration dictionaries.
        :return: A dictionary where each key is a unique pipeline name and each value is the pipeline configuration.
        """
        output_pipelines = {
            f"{elt['pipeline_name']} ({elt['pipeline_type']})": elt
            for elt in input_pipelines
        }
        
        # Add individual functions as single pipelines for multi-function pipelines
        output_pipelines.update({
            f"{fun} ({elt['pipeline_name']})": {
                "pipeline_name": fun,
                "pipeline_type": "single",
                "pipeline_functions": [fun]
            } 
            for elt in input_pipelines if elt["pipeline_type"] == "multiple" for fun in elt["pipeline_functions"]
        })
        
        return output_pipelines
    
    def process_function(self, selected_pipeline, start_date, stop_date, freq, reverse=False, pause=0, retries=0, streamlit=True):
        """
        Executes the selected pipeline with the provided parameters.

        :param selected_pipeline: The name of the pipeline to execute.
        :param start_date: The start date for the pipeline execution.
        :param stop_date: The end date for the pipeline execution.
        :param freq: The frequency of execution (e.g., daily, weekly, monthly).
        :return: A result message detailing the selected pipeline and execution parameters.
        """
        
        if st.session_state.run_custom:
            print("Running custom function")
            pipelines = selected_pipeline.split(";")
            if len(pipelines) == 1:
                pipeline_type = "single"
                pipeline_functions = [selected_pipeline]
            else:
                pipeline_type = "multiple"
                pipeline_functions = pipelines
        else:
            pipeline = st.session_state.pipelines[selected_pipeline]
            pipeline_type = pipeline["pipeline_type"]
            pipeline_functions = pipeline["pipeline_functions"]
 
        duration = datetime.datetime.now()
        if pipeline_type == "single":
            st.session_state.etl.run(
                function=pipeline_functions[0],
                start_date=start_date,
                stop_date=stop_date,
                freq=freq,
                reverse=reverse,
                pause=pause,
                retries=retries,
                streamlit=streamlit
            )
        elif pipeline_type == "multiple":
            st.session_state.etl.run_multiple(
                functions=pipeline_functions,
                start_date=start_date,
                stop_date=stop_date,
                freq=freq,
                reverse=reverse,
                pause=pause,
                retries=retries,
                streamlit=streamlit
            )
        else:
            raise NotImplementedError("Pipeline type not supported.")

        duration = datetime.datetime.now() - duration
        result = f"✅ Pipeline: {selected_pipeline} | Total duration: {duration}"
        return result
    #################################################################################################################################


    #################################################################################################################################
    # Database Management Pages
    #################################################################################################################################
    def db_page_summary(self):
        """
        Data summary interface.
        """
        st.header("DB Summary 🏠")
        client = st.session_state.current_client
        
        tables = client.get_tables(include_all=True, include_size=False)
        nb_schemas = tables['schemaname'].nunique()
        nb_tables = tables['full_tablename'].nunique()
        nb_partitions = tables['partition_count'].sum()
        nb_users = tables['tableowner'].nunique()
        
        
        sessions = client.show_sessions(include_all=True)
        nb_sessions = sessions['session_id'].nunique()
        
        views = client.get_views(include_all=True)
        nb_views = views['viewname'].nunique()
        
        functions = client.get_functions(include_all=True)
        nb_functions = functions['functionname'].nunique()
        
        roles = client.get_roles(include_groups=False)
        nb_roles = roles['rolename'].nunique()
        
        #############################################################################################################################
        st.markdown("---")
        #############################################################################################################################
        col1, col2, col3, col4 = st.columns(4)
    
        with col1:
            st.metric(label="Total Schemas", value=nb_schemas, border=True)
            st.metric(label="Total Sessions", value=nb_sessions, border=True)
            
        with col2:
            st.metric(label="Total Tables", value=nb_tables, border=True)
            st.metric(label="Total Views", value=nb_views, border=True)
            
        with col3:
            st.metric(label="Total Partitions", value=nb_partitions, border=True)
            st.metric(label="Total Functions", value=nb_functions, border=True)
            
        with col4:
            st.metric(label="Total Users", value=nb_users, border=True)
            st.metric(label="Total Roles", value=nb_roles, border=True)
        
        #############################################################################################################################
        st.markdown("---")
        #############################################################################################################################
        col1, col2, col3 = st.columns(3)
        
        with col1:
            counts = tables["schemaname"].value_counts()
            fig1, ax1 = plt.subplots(figsize=(5, 4))
            bars1 = ax1.barh(counts.index, counts.values, color='#1f77b4')
            ax1.bar_label(bars1, padding=3)
            ax1.spines['right'].set_visible(False)
            ax1.spines['top'].set_visible(False)
            plt.title("Tables by Schema")
            st.pyplot(fig1, width='stretch')
            
        with col2:
            counts = tables["tableowner"].value_counts().head(10)
            fig2, ax2 = plt.subplots(figsize=(5, 4))
            bars2 = ax2.barh(counts.index, counts.values, color='#1f77b4')
            ax2.bar_label(bars2, padding=3)
            ax2.spines['right'].set_visible(False)
            ax2.spines['top'].set_visible(False)
            plt.title("Tables by Owner")
            st.pyplot(fig2, width='stretch')
            
        with col3:
            counts = tables["schemaname"].value_counts().head(10)
            fig3, ax3 = plt.subplots(figsize=(5, 4))
            bars3 = ax3.barh(counts.index, counts.values, color='#1f77b4')
            ax3.bar_label(bars3, padding=3)
            ax3.spines['right'].set_visible(False)
            ax3.spines['top'].set_visible(False)
            plt.title("Tables by Schema")
            st.pyplot(fig3, width='stretch')
        
        
        st.markdown("---")
        st.dataframe(tables, width='stretch', hide_index=True)
        
        
        if st.button("Update DB Summary", type="primary", width='stretch'):
            st.balloons()

    def db_page_execution(self):
        """
        Interface d'exécution de fonctions ou procédures.
        """
        st.header("Execution Process ⚡")
        
        st.markdown("---")
        
        #############################################################################################################################
        # Pipeline Selection
        #############################################################################################################################
        col_type, col_custom, col_name = st.columns(3)
        with col_type:
            mode = st.radio("Object Type", options=["Function", "Procedure"], horizontal=True)
        with col_custom:
            st.session_state.run_custom = st.checkbox("Run Custom Function/Procedure", value=True)
            send_mail_custom = st.checkbox("Activate Email Notification", value=True)
        with col_name:
            if st.session_state.run_custom:
                selected_pipeline = st.text_input("Function/Procedure (Use ; to separate multiple)", placeholder="ex: bibox.fn_generate_report")
            else:
                selected_pipeline = st.selectbox("Pipeline", list(st.session_state.pipelines.keys()))

        #############################################################################################################################
        st.markdown("---")
        #############################################################################################################################
            
        with st.form("run_pipeline", clear_on_submit=False, border=False):
            col1, col2, col3 = st.columns(3)
            with col1:
                start_date = st.date_input("Start Date")
                direction = st.selectbox("Execution Order", ["Normal", "Reverse"])
            with col2:
                stop_date = st.date_input("Stop Date")
                pause = st.number_input("Pause (secondes)", min_value=0, value=0)
            with col3:
                freq_label = st.selectbox("Frequency", list(db.utils.FREQ.keys()))
                freq = db.utils.FREQ[freq_label]
                retries = st.number_input("Retries", min_value=0, max_value=3, value=0)

            submit_execution = st.form_submit_button("Run", type="primary", width='stretch')
            if submit_execution:
                if not selected_pipeline:
                    st.error("Please specify the name of the function or procedure.")
                    return

                st.markdown("---")
                
                reverse = (direction == "Reverse")
                
                #####################################################################################################################
                # Execution with error handling
                #####################################################################################################################
                try:
                    result = self.process_function(
                        selected_pipeline,
                        start_date=start_date,
                        stop_date=stop_date,
                        freq=freq,
                        reverse=reverse,
                        pause=pause,
                        retries=retries
                    )

                    status_text = st.empty()
                    st.markdown("---")
                    status_text.write(f"<span style='font-family: Consolas; font-style: bold;'>{result}</span>", unsafe_allow_html=True)
                    st.balloons()
                except OperationalError:
                    st.error("Operational Error !")
                except Exception as e:
                    st.error(f"An error occurred: {e}")
                #####################################################################################################################

    def db_page_sessions(self):
        """
        Affichage et gestion des sessions actives.
        """
        st.header("Current Sessions 🕵️")
        client = st.session_state.current_client
        
        try:
            sessions = client.show_sessions(include_all=False)
            st.dataframe(sessions, width='stretch', hide_index=True)
            
            st.markdown("---")
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Terminate session (PID)")
                with st.form("kill_pid_form", clear_on_submit=False, border=False):
                    pid_to_kill = st.selectbox("Choose a PID", sessions["session_id"].unique())
                    submit_kill = st.form_submit_button("Terminate PID", type="secondary", width='stretch')
                    
                    if submit_kill:
                        target_pid = pid_to_kill
                        st.success(f"Signal sent to PID {target_pid}")
                        client.cancel_query(target_pid)
                        time.sleep(10) # Wait a bit for the session to be killed before refreshing the session list

            with col2:
                st.subheader("Terminate group of sessions")
                st.markdown('<div style="margin-bottom: 0.15rem;"><span style="font-size: 14px; margin-bottom: 0rem;">⚠️ Use with caution !</span></div>', unsafe_allow_html=True)

                if st.button("🔥 Locked Sessions", type="primary", width='stretch'):
                    client.cancel_locked_queries()
                    st.warning("Locked Sessions Terminated.", width='stretch')

                if st.button("🛑 All Sessions", type="primary", width='stretch'):
                    client.cancel_all_queries()
                    st.warning("All Sessions Terminated.", width='stretch')

        except Exception as e:
            st.error(f"Impossible de récupérer les sessions : {e}")

    def db_page_tables(self):
        """
        Exploration des tables, tailles et gestion basique.
        """
        st.header("Tables Management 📂")
        client = st.session_state.current_client
        
        tables = client.get_tables(include_all=False, include_size=False)
        click_on_preview = False
        
        try:
            st.dataframe(tables, width='stretch', hide_index=True)
            
            st.markdown("---")
                        
            selected_table = st.selectbox("Select a table for action", tables['full_tablename'].unique())
            
            c1, c2, c3, c4 = st.columns(4)
            with c1:
                with st.form("preview_table", clear_on_submit=False, border=False):
                    nb_rows = st.number_input("Number of rows to preview", min_value=10, max_value=1000, value=100, step=10)
                    if st.form_submit_button("👀 Show Preview", width='stretch'):
                        click_on_preview = True
                        df_preview = client.sample_table(selected_table, nb_rows)
            
            with c2:
                with st.form("rename_table", clear_on_submit=False, border=False):
                    new_name = st.text_input("Rename Table", placeholder="Enter new name")
                    if st.form_submit_button("📝 Rename", width='stretch'):
                        client.execute(f"ALTER TABLE {selected_table} RENAME TO {new_name}")
                        st.success(f"Table renamed to {new_name}.")

            with c3:
                with st.form("change_schema", clear_on_submit=False, border=False):
                    new_schema = st.selectbox("Select the schema", tables['schemaname'].unique())
                    if st.form_submit_button("📝 Change Schema", width='stretch'):
                        client.execute(f"ALTER TABLE {selected_table} SET SCHEMA {new_schema}")
                        st.success(f"Table moved to schema {new_schema}.")

            with c4:
                st.markdown('<div style="margin-bottom: 0.15rem;"><span style="font-size: 14px; margin-bottom: 0rem;">⚠️ Use with caution !</span></div>', unsafe_allow_html=True)
                if st.button("🗑️ Truncate Table", type="primary", width='stretch'):
                    client.execute(f"TRUNCATE {selected_table}")
                    st.warning(f"Table {selected_table} truncated.")

                if st.button("🗑️ Remove Table", type="primary", width='stretch'):
                    client.execute(f"DROP TABLE {selected_table}")
                    st.error(f"Table {selected_table} deleted.")
            
            if click_on_preview:
                st.dataframe(df_preview, width='stretch', hide_index=True)
                click_on_preview = False

        except Exception as e:
            st.error(f"Erreur d'accès aux tables : {e}")
    
    def db_page_upload_download(self):
        st.header("Data Migration 📥")
        client = st.session_state.current_client
        
        st.markdown("---")
        col1, col2 = st.columns(2)
        
        #############################################################################################################################
        # Upload
        #############################################################################################################################
        with col1:
            file_ok = False
            upload_file_preview = None
            
            st.subheader("Upload data from file")
            file = st.file_uploader("Upload a file", type=["csv", "xlsx"])
            
            # Cache keys
            cached_upload_df_key = "upload_cached_preview_df"
            cached_upload_filename_key = "upload_cached_filename"
            
            if file:
                if cached_upload_filename_key in st.session_state:
                    if st.session_state[cached_upload_filename_key] != file.name:
                        st.session_state.pop(cached_upload_df_key, None)
                        st.session_state.pop(cached_upload_filename_key, None)
            else:
                st.session_state.pop(cached_upload_df_key, None)
                st.session_state.pop(cached_upload_filename_key, None)

            
            #########################################################################################################################
            file_ok = False
            if file and file.name.endswith(".csv"):
                colsep, coldec = st.columns(2)
                with colsep:
                    separator = st.selectbox("Separator", options=[",", ";", "\t", "|"])
                with coldec:
                    decimal = st.selectbox("Decimal Delimiter", options=[".", ","])
                file_ok = True
            elif file and file.name.endswith(".xlsx"):
                sheet_name = st.text_input("Sheet Name")
                file_ok = True
            elif file:
                st.error("Invalid file type. Please upload a valid CSV or Excel file.")
            else:
                st.error("Please upload a valid CSV or Excel file.")
            #########################################################################################################################

            
            #########################################################################################################################
            if file_ok:
                #####################################################################################################################
                if st.button("Preview"):
                    try:
                        if file and file.name.endswith(".csv"):
                            preview_df = pd.read_csv(file, sep=separator, decimal=decimal)
                        else:
                            preview_df = pd.read_excel(file, sheet_name=sheet_name)
                        
                        # Update cache
                        st.session_state[cached_upload_df_key] = preview_df
                        st.session_state[cached_upload_filename_key] = file.name
                    except Exception as e:
                        st.error(e, width='stretch')
                #####################################################################################################################
                
                
                #####################################################################################################################
                if cached_upload_df_key in st.session_state:
                    upload_file_preview = st.session_state[cached_upload_df_key]
                    # Show table
                    st.dataframe(upload_file_preview.head(), width='stretch', hide_index=True)
                
                    # Destination table
                    destination_table = st.text_input("Destination Table")
                    
                    if st.button("Upload Data"):
                        if not destination_table.strip():
                            st.error("Please enter a valid destination table.")
                        else:
                            try:
                                with st.spinner("Loading data into database..."):
                                    db.utils.dataframe_to_db(
                                        dataframe=upload_file_preview,
                                        db_client=client,
                                        destination_table=destination_table,
                                        if_exists="replace", ## Ensure table is truncated if needed
                                        chunksize=50000
                                    )
                                print(f"Data insert to {destination_table}")
                                st.success(f"Data insert to {destination_table}", width='stretch')
                                
                                st.session_state.pop(cached_upload_df_key, None)
                                st.session_state.pop(cached_upload_filename_key, None)
                            except Exception as e:
                                st.error(f"Database insertion failed: {e}")

            file_ok = False
            upload_file_preview = None
        #############################################################################################################################
                

        #############################################################################################################################
        # Export
        #############################################################################################################################
        with col2:
            st.subheader("Export data from table")
            tables = client.get_tables(include_all=True, include_size=False)
            
            selected_table = st.selectbox(
                "Select the table to export", 
                tables['full_tablename'].unique(),
                key="export_target_table_selectbox"
            )
            
            # Save keys
            cached_df_key = "export_cached_preview_df"
            cached_table_key = "export_cached_preview_table_name"
            
            if cached_table_key in st.session_state:
                if st.session_state[cached_table_key] != selected_table:
                    # Clean cache if user selects another table to avoid confusion
                    st.session_state.pop(cached_df_key, None)
                    st.session_state.pop(cached_table_key, None)
            
            # Preview button
            if st.button("Preview table", width='stretch'):
                try:
                    # Sample the table, then store the data and the name of the active table
                    df_preview = client.sample_table(selected_table, 10)
                    st.session_state[cached_df_key] = df_preview
                    st.session_state[cached_table_key] = selected_table
                except Exception as e:
                    st.error(f"Failed to fetch preview: {e}")
            
            # If the cache contains a preview for the active table, display it and the download button
            if cached_df_key in st.session_state:
                df_preview_cached = st.session_state[cached_df_key]
                
                st.dataframe(df_preview_cached, width='stretch', hide_index=True)
                
                # Prepare CSV data and filename for download
                csv_data = df_preview_cached.to_csv(index=False, sep=';', encoding='utf-8')
                file_suffix = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
                filename_csv = f"{selected_table.replace('.', '__')}_{file_suffix}_export.csv"
                
                # Download button
                st.download_button(
                    label='Export Table 💾',
                    data=csv_data,
                    file_name=filename_csv,
                    mime="text/csv",
                    help="Click to download the previewed data as a CSV file",
                    width='stretch'
                )
        #############################################################################################################################
                    
    def db_page_query_console(self):
        """
        Exploration des tables, tailles et gestion basique.
        """
        st.header("Query Console 🎮")
        client = st.session_state.current_client
        
        st.markdown("---")
        query = st.text_area(label="Drop your query (without ; at the end)", value="""select * from bibox.airtime_lu_erecharge_product_lemaplus""")
        
        if st.button("Run query", type="primary", width='stretch'):
            try:
                df = client.read_sql(f"select * from ({query}) foo limit 100")
                st.dataframe(df, width='stretch', hide_index=True)
            except Exception as e:
                st.error(e, width='stretch')
    #################################################################################################################################


    #################################################################################################################################
    # Airflow Management Pages
    #################################################################################################################################
    def airflow_page_summary(self):
        """
        Interface de résumé des données.
        """
        st.header("Airflow Summary 🏠")
        airflow = st.session_state.current_airflow
            
        st.markdown("---")
        
        dags = airflow.get_dags_list(include_all=True)
        dags["owners"] = dags["owners"].astype(str)
        dags["is_paused"] = dags["is_paused"].apply(lambda x: {True: "Paused", False: "Active"}.get(x, "Unknown"))
        dags["folder"] = dags["fileloc"].apply(lambda x: x.split('/')[-2])
        
        try:        
            by_owner, by_state, by_folder, = st.columns(3)
            with by_owner:
                counts = dags["owners"].value_counts()
                fig1, ax1 = plt.subplots(figsize=(5, 4))
                bars1 = ax1.barh(counts.index, counts.values, color='#1f77b4')

                ax1.bar_label(bars1, padding=3)
                ax1.spines['right'].set_visible(False)
                ax1.spines['top'].set_visible(False)
                plt.title("DAGs by Owners")
                st.pyplot(fig1, width='stretch')
                
            with by_state:
                state_counts = dags["is_paused"].value_counts()
                fig2, ax2 = plt.subplots(figsize=(5, 4))
                ax2.pie(
                    state_counts.values, 
                    labels=state_counts.index, 
                    autopct='%1.1f%%', 
                    startangle=140, 
                    colors=['#ff7f0e', '#2ca02c'],
                    pctdistance=0.85
                )
                # Cercle blanc au milieu pour transformer en Donut (plus lisible)
                centre_circle = plt.Circle((0,0), 0.70, fc='white')
                fig2.gca().add_artist(centre_circle)
                ax2.axis('equal') 
                plt.title("Paused (1) vs Active (0)")
                st.pyplot(fig2, width='stretch')
                
            with by_folder:
                folder_counts = dags["folder"].value_counts().head(10) # Top 10 si trop de dossiers
                fig3, ax3 = plt.subplots(figsize=(5, 4))
                bars3 = ax3.barh(folder_counts.index, folder_counts.values, color='#2ca02c')

                ax3.bar_label(bars3, padding=3)
                ax3.spines['right'].set_visible(False)
                ax3.spines['top'].set_visible(False)
                plt.title("DAGs by Folder")
                st.pyplot(fig3, width='stretch')
            
            st.dataframe(dags, width='stretch', hide_index=True)
        except Exception as e:
            st.error(f"Impossible de récupérer les DAGs : {e}")
        
        # if st.button("Refresh Airflow Summary", type="primary", width='stretch'):
        #     pass
    
    def airflow_page_execution(self):
        """
        Interface d'exécution de fonctions ou procédures.
        """
        st.header("Execution Process ⚡")
        airflow = st.session_state.current_airflow
            
        st.markdown("---")
        
        dags = airflow.get_dags_list(include_all=False)
        dags["owners"] = dags["owners"].astype(str)
        dags["is_paused"] = dags["is_paused"].apply(lambda x: {True: "Paused", False: "Active"}.get(x, "Unknown"))
            
        dags["folder"] = dags["fileloc"].apply(lambda x: x.split('/')[-2])

        st.dataframe(dags, width='stretch', hide_index=True)

        st.markdown("---")
        
        dag_details, trigger_dag, run_task, = st.columns(3)
        
        
        with dag_details:
            selected_dag = st.selectbox("Choose a DAG to show", dags["dag_id"].unique())
            if st.button("Show details", type="secondary", width='stretch'):
                details = airflow.get_dag_details(selected_dag, include_tasks=False)
                st.dataframe(details, width='stretch', hide_index=True)
                tasks = airflow.get_dag_tasks(dag_id=selected_dag)#.head(10)
                st.dataframe(tasks, width='stretch', hide_index=True)

        with trigger_dag:
            selected_dag = st.selectbox("Choose a DAG to trigger", dags["dag_id"].unique())
            col1, col2 = st.columns(2)
            with col1:
                start_date = st.date_input("Trigger Start Date").strftime("%Y-%m-%d")
            with col2:
                stop_date = st.date_input("Trigger Stop Date").strftime("%Y-%m-%d")
            if st.button("Trigger", type="secondary", width='stretch'):
                # start_date = (datetime.date.today() - datetime.timedelta(days=0)).strftime("%Y-%m-%d") # D-3
                # end_date = (datetime.date.today() - datetime.timedelta(days=0)).strftime("%Y-%m-%d") # D-1
                log = airflow.trigger_dag(dag_id=selected_dag, start_date=start_date, end_date=stop_date)
                st.json(log)

        with run_task:
            selected_dag = st.selectbox("Choose a DAG to backfill", dags["dag_id"].unique())
            col1, col2 = st.columns(2)
            with col1:
                start_date = st.date_input("Backfill Start Date").strftime("%Y-%m-%d")
            with col2:
                stop_date = st.date_input("Backfill Stop Date").strftime("%Y-%m-%d")
            if st.button("Backfill", type="secondary", width='stretch'):
                # start_date = (datetime.date.today() - datetime.timedelta(days=0)).strftime("%Y-%m-%d") # D-3
                # end_date = (datetime.date.today() - datetime.timedelta(days=0)).strftime("%Y-%m-%d") # D-1
                log = airflow.backfill_dag(dag_id=selected_dag, start_date=start_date, end_date=stop_date, reprocess_behavior="failed")
                st.json(log)
    #################################################################################################################################
#####################################################################################################################################


#####################################################################################################################################
# Start Streamlit App
#####################################################################################################################################
def main():
    """
    Starts the Streamlit app with the specified configuration file and server settings.
    """
    parser = argparse.ArgumentParser(description="Launch DB Analytics Tools UI")
    parser.add_argument("action", choices=["start"], help="Command to run the application")
    parser.add_argument("--config", required=True, help="Path to the configuration file eg. config.json")
    parser.add_argument("--address", default="0.0.0.0", help="Server address to bind Streamlit (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8050, help="Server port to bind Streamlit (default: 8050)")

    args = parser.parse_args()

    # Load the configuration
    with open(args.config, "r") as f:
        config_data = json.load(f)

    # Streamlit script content to initialize the app and run UI.start
    streamlit_script = f"""
import streamlit as st
import json
from db_analytics_tools.webapp import DBAnalyticsUI

# Load the configuration
config_data = {json.dumps(config_data)}

# Start App
app = DBAnalyticsUI(config_data)
app.run()
"""
   
    # Create a temporary Python file for the Streamlit app
    with tempfile.NamedTemporaryFile(delete=False, suffix=".py") as temp_file:
        temp_file.write(streamlit_script.encode("utf-8"))
        temp_filename = temp_file.name

    # Run Streamlit with the temporary file
    try:
        subprocess.run([
            "streamlit", "run", temp_filename,
            "--server.address", args.address,
            "--server.port", str(args.port),
            "--server.headless", "true"
        ])
    finally:
        if os.path.exists(temp_filename):
            os.remove(temp_filename)
#####################################################################################################################################


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        pass
