import subprocess
import json
import os

print(f"Starting {__name__}")
# Step 1: Identify changed files using git diff
changed_files_output = subprocess.check_output(['git', 'diff', '--name-only', 'origin/main'])
changed_files = changed_files_output.decode('utf-8').split('\n')

# Filter for model SQL files
changed_model_files = [file for file in changed_files if file.startswith('models/') and file.endswith('.sql')]

# Extract model names from paths
changed_model_names = [file.split('/')[-1].replace('.sql', '') for file in changed_model_files]

# Step 2 & 3: Find downstream dependencies for each changed model
# Assuming dbt is available in the environment and DBT_PROFILES_DIR is correctly set
downstream_models = []


for model_name in changed_model_names:
    try:
        # Execute the dbt ls command
        downstream_output = subprocess.check_output(['dbt', 'ls', '--select', model_name + '+', '--output', 'json'])
        
        # Split the output by lines and filter out non-JSON parts
        for line in downstream_output.decode('utf-8').split('\n'):
            try:
                # Attempt to decode each line as JSON
                json_line = json.loads(line)
                downstream_models.append(json_line)
            except json.JSONDecodeError:
                # Skip lines that are not valid JSON
                continue
    except subprocess.CalledProcessError as e:
        print(f"Error executing dbt ls for model {model_name}: {e.output.decode()}")
        continue
    
# Remove duplicates
unique_downstream_models = set([x.get("original_file_path") for x in downstream_models])

# Print models for CI/CD usage
print("DOWNSTREAM MODELS:")
print(' '.join(unique_downstream_models))

for model in unique_downstream_models:
    subprocess.check_output(['dbt', 'run', '--select', 'model', '--vars', '{"schema_override": "dbt_test"}'])
    print(f"Finished running {model}")
