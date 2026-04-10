import re
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any
import os
from sqlalchemy import create_engine, text

#db connection
postgresql_url = 'postgres connection string'
engine = create_engine(postgresql_url)

class XERParser:

    def __init__(self, file_path: str):
        self.file_path = file_path
        self.data = {}
        self.tables = {}
        self.project_info = {}  

    def parse(self):
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"xer file not found: {self.file_path}")
        
        with open(self.file_path, 'r', encoding='utf-8', errors='ignore') as file:
            content = file.read()
        self._parse_content(content)

        return {
            'project_info': self.project_info,
            'tables': self.tables,
            'data': self.data
        } 
    
    def _parse_content(self, content):
        lines = content.split('\n')
        current_table = None
        current_fields = []

        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            if line.startswith('%T'):
                table_name = line.split('\t')[1]
                current_table = table_name
                current_fields = []
                self.tables[table_name] = []

            elif line.startswith('%F') and current_table:
                fields = line.split('\t')[1:]
                current_fields = fields

            elif line.startswith('%R') and current_table and current_fields:
                values = line.split('\t')[1:]
                row_data = {}

                for i, field in enumerate(current_fields):
                    if i < len(values):
                        row_data[field] = values[i]
                    else:
                        row_data[field] = ''
                self.tables[current_table].append(row_data)

            elif line.startswith('ERMHDR'):
                parts = line.split('\t')
                if len(parts) >= 8:
                    self.project_info = {
                        'version': parts[1],
                        'export_date': parts[2],
                        'project_name': parts[3],
                        'user_name': parts[4],
                        'database': parts[5],
                        'description': parts[6],
                        'currency': parts[7]
                    }
    
    def get_project_data(self) -> pd.DataFrame:

        if 'PROJECT' not in self.tables:
            return pd.DataFrame()
            
        df = pd.DataFrame(self.tables['PROJECT'])
        
        if df.empty:
            return df
            
    
        numeric_columns = ['proj_id']
        
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
 
        date_columns = ['create_date','last_recalc_date','plan_start_date','plan_end_date']
        
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                
        return df
    
    def get_wbs_data(self) -> pd.DataFrame:
        if 'PROJWBS' not in self.tables:
            return pd.DataFrame()
            
        df = pd.DataFrame(self.tables['PROJWBS'])
        
        if df.empty:
            return df
            
 
        numeric_columns = ['wbs_id', 'proj_id']
        
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        return df
    
    def get_task_data(self) -> pd.DataFrame:

        if 'TASK' not in self.tables:
            return pd.DataFrame()
            
        df = pd.DataFrame(self.tables['TASK'])
        
        if df.empty:
            return df

        date_columns = ['act_start_date','act_end_date','late_start_date','late_end_date',
        'early_start_date','early_end_date','restart_date','target_start_date',
        'target_end_date','update_date']
        
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                
     
        numeric_columns = ['task_id', 'proj_id', 'wbs_id', 'clndr_id', 
                          'phys_complete_pct']
        
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                
        return df
    
    def get_calendar_data(self) -> pd.DataFrame:
        if 'CALENDAR' not in self.tables:
            return pd.DataFrame()
            
        df = pd.DataFrame(self.tables['CALENDAR'])
        
        if df.empty:
            return df

        if 'clndr_id' in df.columns and 'clndr_data' in df.columns:
            df = df[['clndr_id', 'clndr_data']]
            
           
            df['clndr_id'] = pd.to_numeric(df['clndr_id'], errors='coerce')
        
        return df


    def get_taskpred_data(self) -> pd.DataFrame:
        if 'TASKPRED' not in self.tables:
            return pd.DataFrame()
            
        df = pd.DataFrame(self.tables['TASKPRED'])
        
        if df.empty:
            return df
            
        return df
    
    # export tables to excel files 
    def export_to_separate_files(self, output_dir: str = "Data"):

        os.makedirs(output_dir, exist_ok=True)
        
        
        task_df = self.get_task_data()
        wbs_df = self.get_wbs_data()
        project_df = self.get_project_data()
        calendar_df = self.get_calendar_data() 
        
       
        task_file = os.path.join(output_dir, "task.xlsx")
        if not task_df.empty:
            task_df.to_excel(task_file, index=False)
        else:
            print("No data found")
        
    
        wbs_file = os.path.join(output_dir, "wbs.xlsx")
        if not wbs_df.empty:
            wbs_df.to_excel(wbs_file, index=False)
        else:
            print("No data found")
        
       
        project_file = os.path.join(output_dir, "project.xlsx")
        if not project_df.empty:
            project_df.to_excel(project_file, index=False)
        else:
            print("No data found")
        
   
        calendar_file = os.path.join(output_dir, "calendar.xlsx")
        if not calendar_df.empty:
            calendar_df.to_excel(calendar_file, index=False)
        else:
            print("No data found")
      

        
        if not project_df.empty:
            print(f"\nProject IDs found: {project_df['proj_id'].unique()}")
        
        return {
            'task_file': task_file if not task_df.empty else None,
            'wbs_file': wbs_file if not wbs_df.empty else None,
            'project_file': project_file if not project_df.empty else None,
            'task_records': len(task_df),
            'wbs_records': len(wbs_df),
            'project_records': len(project_df)
        }
   
    # export tables to db 
    def export_to_db(self, engine, is_baseline=False):
        try:
            task_df = self.get_task_data()
            wbs_df = self.get_wbs_data()
            project_df = self.get_project_data()
            calendar_df = self.get_calendar_data()
            
            
            if project_df.empty:
                raise Exception("No project data found in XER file")
            
            proj_id = int(project_df['proj_id'].iloc[0])

            with engine.connect() as conn:
                with conn.begin():

                    version_query = text("""
                        SELECT COALESCE(MAX(version), 0) as max_version
                        FROM public."PROJWBS"
                        WHERE proj_id = :proj_id AND is_baseline = :is_baseline
                    """)
                    result = conn.execute(version_query, {"proj_id": proj_id, "is_baseline": is_baseline})
                    max_version = result.fetchone()[0]
                    new_version = max_version + 1                    
                
                    #project
                    project_auto_id = None  
                    if not project_df.empty:
                        for _,row in project_df.iterrows():
                            query = text("""
                            INSERT INTO public."PROJECT"
                            (proj_id,proj_short_name,last_recalc_date,clndr_id,plan_start_date,plan_end_date,is_baseline,version)
                            VALUES (:proj_id, :proj_short_name, :last_recalc_date, :clndr_id, :plan_start_date, :plan_end_date, :is_baseline, :version)
                            RETURNING "ID"
                            """)
                            row_dict = row.to_dict()
                            for key, value in row_dict.items():
                                if pd.isna(value):
                                    row_dict[key] = None
                            row_dict['is_baseline'] = is_baseline
                            row_dict['version'] = new_version
                            result = conn.execute(query, row_dict)
                            project_auto_id = result.fetchone()[0]  
                            print(f"  - PROJECT inserted with auto-generated ID: {project_auto_id}")
                    else:
                        print("no project data was found")
                    
                    
                    if not project_auto_id:
                        raise Exception("Failed to get auto-generated PROJECT ID")
                    
                    #calendar
                    if not calendar_df.empty:
                        for _,row in calendar_df.iterrows():
                                query = text("""
                                INSERT INTO public."CALENDAR"
                                (clndr_id,clndr_data,is_baseline,version,project_id)
                                VALUES (:clndr_id, :clndr_data, :is_baseline, :version, :project_id)
                                """)
                                row_dict = row.to_dict()
                                for key, value in row_dict.items():
                                    if pd.isna(value):
                                        row_dict[key] = None
                                row_dict['is_baseline'] = is_baseline
                                row_dict['version'] = new_version
                                row_dict['project_id'] = project_auto_id  
                                conn.execute(query, row_dict)
                    else:
                        print("no calendar data was found")
                    
                    #wbs
                    if not wbs_df.empty:
                        for _,row in wbs_df.iterrows():
                            query = text("""
                            INSERT INTO public."PROJWBS"
                            (wbs_id,proj_id,wbs_name,proj_node_flag,status_code,parent_wbs_id,is_baseline,version,project_id)
                            VALUES (:wbs_id, :proj_id, :wbs_name, :proj_node_flag, :status_code, :parent_wbs_id, :is_baseline, :version, :project_id)
                            """)
                            row_dict = row.to_dict()
                            for key, value in row_dict.items():
                                if pd.isna(value):
                                    row_dict[key] = None
                            row_dict['is_baseline'] = is_baseline
                            row_dict['version'] = new_version
                            row_dict['project_id'] = project_auto_id  
                            conn.execute(query, row_dict)
                    else:
                        print("no wbs data was found")
                    
                    #task
                    if not task_df.empty:
                        for _,row in task_df.iterrows():
                            query = text("""
                            INSERT INTO public."TASK"
                            (task_id,proj_id,wbs_id,clndr_id,task_name,phys_complete_pct,status_code,task_code,remain_work_qty,target_work_qty,act_start_date,act_end_date,late_start_date,late_end_date,early_start_date,early_end_date,restart_date,target_start_date,target_end_date,update_date,task_type,is_baseline,version,project_id)
                            VALUES (:task_id, :proj_id, :wbs_id, :clndr_id, :task_name, :phys_complete_pct, :status_code, :task_code, :remain_work_qty, :target_work_qty, :act_start_date, :act_end_date, :late_start_date, :late_end_date, :early_start_date, :early_end_date, :restart_date, :target_start_date, :target_end_date, :update_date, :task_type, :is_baseline, :version, :project_id)
                            """)
                            row_dict = row.to_dict()
                            for key, value in row_dict.items():
                                if pd.isna(value):
                                    row_dict[key] = None
                            row_dict['is_baseline'] = is_baseline
                            row_dict['version'] = new_version
                            row_dict['project_id'] = project_auto_id  
                            conn.execute(query, row_dict)
                    
                    return proj_id

        except Exception as e:
            print(f"Error exporting to db: {str(e)}")
            return None
                            
            
def parse_xer_file(file_path: str) -> XERParser:

    parser = XERParser(file_path)
    parser.parse()
    return parser


def process_xer_file(xer_file_path: str, output_dir: str = "Data"):


    if not os.path.exists(xer_file_path):
        print(f"XER file not found: {xer_file_path}")
        return None
    
    try:
        parser = parse_xer_file(xer_file_path)
        
        result = parser.export_to_db(engine)
        return result
        
    except Exception as e:
        print(f"Error processing {xer_file_path}: {str(e)}")
        return None



if __name__ == "__main__":
 
    xer_files = [
        r"C:\Users\lalessa\Downloads\DC - Sustainability update 120226.xer"
    ]
    
    for xer_file in xer_files:
        result = process_xer_file(xer_file, "Data")
        

    



