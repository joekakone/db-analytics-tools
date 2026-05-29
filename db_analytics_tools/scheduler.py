# coding : utf-8

"""
    DB Analytics Tools Cron Manager
    A utility class to manage system crontab entries using unique identifiers in comments.
"""


#####################################################################################################
import subprocess
import os
import re

import pandas as pd
#####################################################################################################


class CronManager:
    """
    A utility class to manage system crontab entries using unique identifiers in comments.
    """

    def __init__(self):
        self.app_id_prefix = "DB_TOOLS_ID:"
        self.app_cmd_prefix = "db_cli"
        
    def _parse_cron_line(self, line):
        """
        Splits a cron line into three distinct parts: ID, Schedule, and Command.
        """
        
        # 1. Extract Schedule (the first 5 fields) and Command
        job_schedule = line.split(self.app_cmd_prefix)[0]
        
        # 2. Extract ID
        job_id = self.app_id_prefix + line.split(self.app_id_prefix)[-1]
        
        # 3. Extract CMD
        job_cmd = self.app_cmd_prefix + line.split(self.app_cmd_prefix)[-1].split(self.app_cmd_prefix)[0]

        return {
            "id": job_id,
            "schedule": job_schedule,
            "cmd": job_cmd,
            "raw": line
        }

    def _get_all_lines(self):
        """
        Retrieves all current crontab lines.
        """
        try:
            output = subprocess.check_output("crontab -l", shell=True, stderr=subprocess.STDOUT)
            return output.decode().splitlines()
        except subprocess.CalledProcessError:
            return []

    def _write_crontab(self, lines):
        """
        Writes the provided list of lines back to the system crontab.
        """
        temp_file = "temp_cron_dispatch.txt"
        try:
            with open(temp_file, "w") as f:
                f.write("\n".join(lines) + "\n")
            subprocess.check_call(f"crontab {temp_file}", shell=True)
        finally:
            if os.path.exists(temp_file):
                os.remove(temp_file)

    def create(self, cmd, schedule, comment="python_job", args=None):
        """
        Adds a new cron job with a specific schedule and unique comment ID.
        """
        lines = self._get_all_lines()
        
        # Prevent duplicate IDs
        if any(f"{self.app_id_prefix}{comment}" in line for line in lines):
            # raise ValueError(f"Cron job with ID '{comment}' already exists.")
            print(f"Cron job with ID '{comment}' already exists.")
            return False

        full_cmd = cmd
        if args:
            full_cmd = f"{cmd} {args}"

        # Format: [schedule] [command] # DB_TOOLS_ID:comment
        new_entry = f"{schedule} {full_cmd} # {self.app_id_prefix}{comment}"
        lines.append(new_entry)
        
        self._write_crontab(lines)
        print(f"Cron job '{comment}' created successfully.")
        
        return True

    def read(self, comment=None):
        """
        Lists all cron jobs or filters by the specific comment ID.
        """
        lines = self._get_all_lines()
        jobs = []
        
        for line in lines:
            if not line.strip() or line.startswith("# "):
                continue
                
            # Extract ID from the end of the line
            match = re.search(f"# {self.app_id_prefix}(.+)$", line)
            if match:
                job_id = match.group(1).strip()
                # If comment is specified, only return matching ID
                if comment and job_id != comment:
                    continue
                
                jobs.append(self._parse_cron_line(line))
                
        if not comment:
            return pd.DataFrame(jobs)
        return jobs

    def update(self, comment, new_cmd=None, new_schedule=None):
        """
        Updates an existing cron job identified by its comment ID.
        """
        lines = self._get_all_lines()
        updated = False
        new_lines = []

        for line in lines:
            if f"{self.app_id_prefix}{comment}" in line:
                # Parse existing line
                parts = line.split(f" # {self.app_id_prefix}")
                config_part = parts[0].strip()
                
                # Split schedule and command (simple split on first 5 spaces)
                config_split = config_part.split(None, 5)
                current_schedule = " ".join(config_split[:5])
                current_cmd = config_split[5]

                final_schedule = new_schedule if new_schedule else current_schedule
                final_cmd = new_cmd if new_cmd else current_cmd
                
                new_lines.append(f"{final_schedule} {final_cmd} # {self.app_id_prefix}{comment}")
                updated = True
            else:
                new_lines.append(line)

        if updated:
            self._write_crontab(new_lines)
        return updated

    def disable(self, comment):
        """
        Disables a cron job entry matching the unique comment ID.
        """
        lines = self._get_all_lines()
        updated = False
        new_lines = []

        for line in lines:
            if (f"{self.app_id_prefix}{comment}" in line) and (not line.strip().startswith("#")):
                new_line = f"# {line}".strip()
                new_lines.append(new_line)
                updated = True
            else:
                new_lines.append(line)

        if updated:
            self._write_crontab(new_lines)
        return updated

    def enable(self, comment):
        """
        Enables a cron job entry matching the unique comment ID.
        """
        lines = self._get_all_lines()
        updated = False
        new_lines = []

        for line in lines:
            if (f"{self.app_id_prefix}{comment}" in line) and (line.strip().startswith("#")):
                new_line = f"# {line}".strip()
                new_lines.append(new_line)
                updated = True
            else:
                new_lines.append(line)

        if updated:
            self._write_crontab(new_lines)
        return updated

    def delete(self, comment):
        """
        Removes a cron job entry matching the unique comment ID.
        """
        lines = self._get_all_lines()
        new_lines = [line for line in lines if f"{self.app_id_prefix}{comment}" not in line]
        
        if len(new_lines) != len(lines):
            self._write_crontab(new_lines)
            return True
        return False

# --- Example Usage ---
if __name__ == "__main__":
    manager = CronManager()
    
    command = "db_cli --engine greenplum --host XX.XXX.XX.XXX --port 5432 --database cdrfw --user joekakone --password Axian2580 --start 2026-01-01 --stop 2026-05-01 --functions bibox.fn_gros_ad_lu_agents bibox.fn_gros_ad_lu_agents_month_alignement --frequency m"
    
    # 1. Create
    manager.create(command, "0 0 1 * *", comment="db_sync_weekly")
    
    # 2. Read all
    df = manager.read()
    print(df.columns, df.shape, df.index)
    print("All Jobs:\n", df.reset_index(drop=True).T)
    
    # 3. Update schedule to every Monday
    manager.update("db_sync_monthly", new_schedule="0 0 * * 1")
    
    # 4. Delete
    # manager.delete("db_sync_monthly")
