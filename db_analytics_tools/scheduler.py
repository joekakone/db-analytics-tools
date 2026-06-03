# coding : utf-8

"""
    DB Analytics Tools Cron Manager
    A utility class to manage system crontab entries using unique identifiers in comments.
"""

#####################################################################################################
# Package Imports
#####################################################################################################
import os
import re
import subprocess
import tempfile
import logging

import pandas as pd
#####################################################################################################


#####################################################################################################
# Set up logging configuration
#####################################################################################################
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    force=True
)
logger = logging.getLogger("CronManager")
#####################################################################################################


#####################################################################################################
# CronManager Class Definition
#####################################################################################################
class CronManager:
    """
    A utility class to manage system crontab entries using unique identifiers in comments.
    """

    def __init__(self):
        self.app_id_prefix = "DB_TOOLS_ID:"
        self.app_cmd_prefix = "db_cli"
        logger.info("CronManager initialized successfully.")

    def _get_id_pattern(self, comment):
        """
        Returns a compiled regex pattern to find the specific comment ID,
        supporting variable spacing and comment hashes.
        """
        return re.compile(r"#\s*" + re.escape(self.app_id_prefix) + r"\s*" + re.escape(comment) + r"\s*$")

    def _parse_cron_line(self, line):
        """
        Parses a cron line into clean structured components using precise string splits.
        """
        clean_line = line.strip()
        is_disabled = clean_line.startswith("#")
        content = clean_line.lstrip("#").strip()
        
        # Locate the structural application token identifier
        if f"{self.app_id_prefix}" in content:
            cmd_part, id_part = content.split(f"{self.app_id_prefix}", 1)
            job_id = id_part.strip()
        else:
            return {
                "id": "UNKNOWN", "schedule": "", "cmd": clean_line, 
                "log": "", "disabled": is_disabled, "raw": line
            }

        # Separate schedule (first 5 space-separated blocks) from the command core
        chunks = cmd_part.strip().rstrip("#").strip().split(None, 5)
        if len(chunks) < 6:
            return {
                "id": f"{self.app_id_prefix}{job_id}", "schedule": cmd_part.strip(), 
                "cmd": "", "log": "", "disabled": is_disabled, "raw": line
            }
            
        schedule = " ".join(chunks[:5])
        rest = chunks[5].strip()
        
        cmd = rest
        log = ""
        if ">>" in rest:
            cmd_before_log, log_after = rest.split(">>", 1)
            cmd = cmd_before_log.strip()
            log = log_after.strip()

        return {
            "id": f"{self.app_id_prefix}{job_id}",
            "schedule": schedule,
            "cmd": cmd,
            "log": log,
            "disabled": is_disabled,
            "raw": line
        }

    def _get_all_lines(self):
        """
        Retrieves all current crontab lines safely.
        """
        try:
            output = subprocess.check_output("crontab -l", shell=True, stderr=subprocess.STDOUT)
            lines = output.decode().splitlines()
            logger.debug(f"Successfully retrieved {len(lines)} lines from system crontab.")
            return lines
        except subprocess.CalledProcessError as e:
            logger.warning(f"No crontab found or command failed. Returning empty list. Details: {e.output.decode().strip()}")
            return []

    def _write_crontab(self, lines):
        """
        Writes the provided list of lines back to the system crontab using a secure NamedTemporaryFile.
        """
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix="_cron.txt") as temp_file:
            temp_file.write("\n".join(lines) + "\n")
            temp_path = temp_file.name

        try:
            subprocess.check_call(f"crontab {temp_path}", shell=True)
            logger.info("System crontab updated successfully.")
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to execute crontab update command. Error: {e}")
            raise e
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

    def create(self, cmd, schedule, comment="python_job", args=None):
        """
        Adds a new cron job with a specific schedule and unique comment ID.
        """
        logger.info(f"Attempting to create cron job with ID target: '{comment}'")
        lines = self._get_all_lines()
        pattern = self._get_id_pattern(comment)
        
        if any(pattern.search(line) for line in lines):
            logger.error(f"Creation failed: Cron job with ID '{comment}' already exists.")
            return False

        full_cmd = f"{cmd} {args}" if args else cmd
        new_entry = f"{schedule} {full_cmd} >> {comment}.log # {self.app_id_prefix}{comment}"
        lines.append(new_entry)
        
        self._write_crontab(lines)
        logger.info(f"Cron job '{comment}' added and committed successfully.")
        return True

    def read(self, comment=None):
        """
        Lists all custom tool cron jobs or filters by a specific comment ID.
        """
        if comment:
            logger.info(f"Filtering crontab lines for target ID: '{comment}'")
        else:
            logger.info("Reading all tool-managed crontab entries.")

        lines = self._get_all_lines()
        jobs = []
        
        for line in lines:
            if not line.strip() or self.app_id_prefix not in line:
                continue
                
            parsed = self._parse_cron_line(line)
            clean_id = parsed["id"].replace(self.app_id_prefix, "")
            
            if comment and clean_id != comment:
                continue
            jobs.append(parsed)
                
        logger.info(f"Found {len(jobs)} jobs matching criteria.")
        if not comment:
            return pd.DataFrame(jobs)
        return jobs

    def update(self, comment, new_cmd=None, new_schedule=None):
        """
        Updates an existing cron job's schedule or command identified by its comment ID.
        """
        logger.info(f"Initiating update configuration request for job ID: '{comment}'")
        lines = self._get_all_lines()
        pattern = self._get_id_pattern(comment)
        updated = False
        new_lines = []

        for line in lines:
            if pattern.search(line):
                parsed = self._parse_cron_line(line)
                
                final_schedule = new_schedule if new_schedule else parsed["schedule"]
                final_cmd = new_cmd if new_cmd else parsed["cmd"]
                final_log_str = f" >> {parsed['log']}" if parsed["log"] else ""
                
                disabled_prefix = "# " if parsed["disabled"] else ""
                updated_entry = f"{disabled_prefix}{final_schedule} {final_cmd}{final_log_str} # {self.app_id_prefix}{comment}"
                
                new_lines.append(updated_entry)
                updated = True
                logger.info(f"Target line matching ID '{comment}' updated inline.")
                logger.debug(f"Old line: {line} -> New line: {updated_entry}")
            else:
                new_lines.append(line)

        if updated:
            self._write_crontab(new_lines)
            logger.info(f"Modifications for job '{comment}' successfully written to the system.")
        else:
            logger.warning(f"Update skipped: No cron job found with ID '{comment}'.")
            
        return updated

    def disable(self, comment):
        """
        Disables an active cron job entry matching the unique comment ID.
        """
        logger.info(f"Request received to DISABLE cron job ID: '{comment}'")
        lines = self._get_all_lines()
        pattern = self._get_id_pattern(comment)
        updated = False
        new_lines = []

        for line in lines:
            if pattern.search(line):
                clean_line = line.strip()
                if not clean_line.startswith("#"):
                    new_lines.append(f"# {clean_line}")
                    updated = True
                    logger.info(f"Cron job ID '{comment}' has been disabled.")
                    continue
            new_lines.append(line)

        if updated:
            self._write_crontab(new_lines)
        else:
            logger.warning(f"Disable targeted line skipped: Job '{comment}' was already disabled or not found.")
            
        return updated

    def enable(self, comment):
        """
        Enables a commented/disabled cron job entry matching the unique comment ID.
        """
        logger.info(f"Request received to ENABLE cron job ID: '{comment}'")
        lines = self._get_all_lines()
        pattern = self._get_id_pattern(comment)
        updated = False
        new_lines = []

        for line in lines:
            if pattern.search(line):
                clean_line = line.strip()
                if clean_line.startswith("#"):
                    new_lines.append(clean_line.lstrip("#").strip())
                    updated = True
                    logger.info(f"Cron job ID '{comment}' has been re-enabled successfully.")
                    continue
            new_lines.append(line)

        if updated:
            self._write_crontab(new_lines)
        else:
            logger.warning(f"Enable targeted line skipped: Job '{comment}' was already active or not found.")
            
        return updated

    def delete(self, comment):
        """
        Removes a cron job entry matching the unique comment ID entirely.
        """
        logger.info(f"Request received to DELETE cron job ID: '{comment}' permanently.")
        lines = self._get_all_lines()
        pattern = self._get_id_pattern(comment)
        
        new_lines = [line for line in lines if not pattern.search(line)]
        
        if len(new_lines) != len(lines):
            self._write_crontab(new_lines)
            logger.info(f"Cron job ID '{comment}' removed from crontab table.")
            return True
            
        logger.warning(f"Deletion skipped: No target line found with ID '{comment}'.")
        return False
