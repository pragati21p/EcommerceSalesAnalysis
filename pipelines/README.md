# Pipeline logic

## YAML
Top level and entry point in the Azure DevOps pipeline logic are the `.yml` files which define the pipeline sequence.
The `.yml` files call the actual build steps as _script task_ which are defined in the `.sh` shell script files.

### BASH
Two of the shell scripts (for cluster or job creation in the _Databricks Workspace_) reference the `.config.json` which contain the configuration parameters.

#### JSON
The `.config.json` files trigger the corresponding pipelines.
Any change to the configuration JSON files will trigger an update of the corresponding cluster or job.
