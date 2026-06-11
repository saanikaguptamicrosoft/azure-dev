# Troubleshooting Guide — `azd ai training` & `azd ai models`

---

## Training Extension (`azd ai training`)

### Setup / Init

#### `environment not configured. Run 'azd ai training init' first`
**Cause:** You ran a `job` subcommand before initializing the extension.  
**Fix:** Run `azd ai training init` (interactive wizard) or pass `--subscription` and `--project-endpoint` together on any job command.

#### `--subscription (-s) and --project-endpoint (-e) must be provided together`
**Cause:** You passed one of the two flags without the other.  
**Fix:** Always pass both flags together, or omit both and let the stored environment values be used.

#### `invalid endpoint URL: expected format /api/projects/{project-name}`
**Cause:** The value passed to `--project-endpoint` doesn't match the expected URL shape.  
**Fix:** Use the format `https://<account>.services.ai.azure.com/api/projects/<project-name>`. Ensure there is no trailing slash after the project name.

#### `could not find Cognitive Services account '<account>' in subscription '<sub>'`
**Cause:** The account name extracted from the endpoint URL doesn't exist in the given subscription, or you don't have access to it.  
**Fix:** Verify the account name and subscription ID. Confirm you have at least Reader access to the Cognitive Services resource.

#### `could not find project '<project>' under account '<account>'`
**Cause:** The project name extracted from the endpoint URL doesn't exist under that account.  
**Fix:** Check the project name in Azure AI Foundry portal. Make sure you are using the correct account.

#### `azd environment not found, please create an environment (azd env new) and try again`
**Cause:** No active `azd` environment exists in the current directory.  
**Fix:** Run `azd env new` to create one, then retry.

---

### Job Validation (`azd ai training job validate` / pre-flight during `job submit`)

Validation runs offline before any network calls. Errors are shown as `✗ [Error] <field>: <message>`.

#### `required field is missing` on `type`, `command`, or `compute`
**Cause:** One of the three required top-level YAML fields is absent.  
**Fix:** Ensure `type`, `command`, and `compute` are all present in your job YAML.

#### `code: git paths are not supported`
**Cause:** The `code` field contains a `git://` or `git+https://` URL.  
**Fix:** Clone the repository locally and point `code` at the local directory, or use an `azureml://` datastore URI.

#### `code: local path does not exist: '<path>'`
**Cause:** The path in the `code` field doesn't exist relative to the YAML file.  
**Fix:** Check the path. Relative paths are resolved against the directory containing the YAML file.

#### `inputs.<name>.type: type is required (e.g. uri_folder, uri_file etc)`
**Cause:** An input that carries a path or URI has no `type` declared. The backend will reject the job with `Unexpected JobInputType in request body: []`.  
**Fix:** Add `type: uri_folder` or `type: uri_file` to the input.

#### `outputs.<name>: output name 'default' is reserved by the system`
**Cause:** You named an output `default`, which is reserved by the backend.  
**Fix:** Rename the output to anything other than `default`.

#### `outputs.<name>.type: type is required`
**Cause:** An output has no `type` declared.  
**Fix:** Add `type: uri_folder` or `type: uri_file` to the output.

#### `command: command references '${{inputs.<key>}}' but '<key>' is not defined in inputs`
**Cause:** The command string references an input placeholder that doesn't have a matching entry in the `inputs` section.  
**Fix:** Either add the missing input definition, or fix the typo in the placeholder name.

#### `command: Incorrect placeholder format — use '${{inputs.<key>}}' instead`
**Cause:** You wrote `{inputs.key}` instead of `${{inputs.key}}`. Single-brace placeholders are not resolved by the backend.  
**Fix:** Use the `${{ }}` syntax.

#### `services.<name>.ssh_public_keys: ssh_public_keys is required when type is 'ssh'`
**Cause:** An SSH service entry is missing `ssh_public_keys`. Without a public key, the SSH service starts but is unusable.  
**Fix:** Add your public key under `ssh_public_keys` in the service definition.

#### `services.<name>.type: type "<value>" is not supported`
**Cause:** The service type value is not one of the supported types.  
**Fix:** Use one of: `ssh`, `jupyter_lab`, `tensor_board`, `vs_code`, `custom`.

#### `distribution.type: type is required when distribution is specified`
**Cause:** A `distribution` block is present but the `type` field is empty.  
**Fix:** Set `type` to one of: `pytorch`, `tensorflow`, `mpi`, `ray`.

#### `distribution.port: port <N> is out of range; expected 1–65535`
**Cause:** A Ray distribution `port` or `dashboard_port` value is outside the valid TCP port range.  
**Fix:** Use a port number between 1 and 65535.

---

### Job Submit (`azd ai training job submit`)

#### `--file (-f) is required`
**Cause:** No YAML file was provided.  
**Fix:** Pass `--file <path-to-job.yaml>`.

#### `project "<name>" has no User-Assigned Managed Identity. Without it, 'azd ai training job submit' will fail.`
**Cause:** The Foundry project doesn't have a User-Assigned Managed Identity (UAMI) attached. Training job submission requires one.  
**Fix:** In Azure portal, assign a User-Assigned Managed Identity to the Azure AI Foundry project. See [Managed Identities in Azure AI Foundry](https://learn.microsoft.com/en-us/azure/ai-studio/concepts/rbac-ai-studio).

#### `failed to resolve job definition: failed to resolve compute '<name>'`
**Cause:** The `compute` field in the YAML references a compute cluster that doesn't exist or isn't accessible.  
**Fix:** Verify the compute cluster name using `az cognitiveservices account compute list -n <account> -g <rg>`.

#### `failed to initialize azcopy`
**Cause:** `azcopy` could not be found or auto-installed. This is required to upload code and input data.  
**Fix:** Install `azcopy` manually and either place it in your `PATH` or pass `--azcopy-path <path>`. Alternatively, ensure your machine has internet access for the auto-installer.

#### `failed to create job: <API error>`
**Cause:** The backend rejected the job. The error message usually includes an HTTP status code and details.  
**Fix:** Check the message for specifics. Common causes: incorrect `compute` ARM ID, invalid environment reference, or missing UAMI permissions.

---

### Job Download (`azd ai training job download`)

#### `This job is in state <state>. Download is allowed only in states: Completed, Failed, Canceled, NotResponding, Paused`
**Cause:** You tried to download a job that hasn't finished yet.  
**Fix:** Wait for the job to reach a terminal state. Monitor with `azd ai training job show --name <name>`.

#### `no output named "<name>" on job "<job>". Available outputs: [...]`
**Cause:** The `--output-name` value doesn't match any output on the job.  
**Fix:** Use one of the listed available output names, or use `--all` to download everything.

#### `--all and --output-name cannot be used together`
**Cause:** Both flags were passed simultaneously.  
**Fix:** Use `--all` for all outputs, or `--output-name <name>` for a single one — not both.

---

### SSH (`azd ai training job connect-ssh`)

#### `ssh client not found in PATH; please install OpenSSH client`
**Cause:** The `ssh` binary isn't available on your machine.  
**Fix:** Install OpenSSH. On Windows, enable the "OpenSSH Client" optional feature under Settings → Apps → Optional Features.

#### `job "<name>" is in terminal state "<state>"; SSH is only available while the job is Running`
**Cause:** The job is already finished; the container no longer exists.  
**Fix:** SSH is only possible while the job status is `Running`.

#### `the node <N> of the job does not have services; ensure that the job has services`
**Cause:** The job was submitted without any services defined in the YAML.  
**Fix:** Re-submit the job with an SSH service block in the YAML.

#### `please ensure that the job is ssh enabled on node '<N>'`
**Cause:** The job has services, but none of them is of type `SSH` on that node.  
**Fix:** Verify the `services` block in your job YAML includes a service with `type: ssh`. Re-submit if missing.

#### `please ensure that ssh service at node '<N>' has the status as 'Running'. The current status is '<status>'`
**Cause:** The SSH service exists but hasn't started yet (typically takes 30–120 s after the job enters Running).  
**Fix:** Wait and retry. Use `azd ai training job show-services --name <name>` to monitor SSH status.

#### `the ssh JobService.properties is missing ProxyEndpoint`
**Cause:** The service is Running but the backend hasn't populated the proxy endpoint yet, or the SSH service configuration is incomplete.  
**Fix:** Wait a few seconds and retry. If the issue persists, re-submit the job.

---

## Models Extension (`azd ai models`)

### Setup

#### `invalid project endpoint URL: expected format https://{account}.services.ai.azure.com/api/projects/{project}`
**Cause:** The `--project-endpoint` value is malformed.  
**Fix:** Use the format `https://<account>.services.ai.azure.com/api/projects/<project-name>`.

#### `invalid project endpoint URL: scheme must be https`
**Cause:** An `http://` URL was provided.  
**Fix:** Use `https://`.

#### `--project-endpoint is required when azd is not available.`
**Cause:** The command was run outside an `azd`-initialized directory and no `--project-endpoint` was passed.  
**Fix:** Either run from an initialized `azd` project directory, or pass `--project-endpoint`.

---

### Model Create (`azd ai models create`)

#### `either --source or --source-file is required`
**Cause:** Neither `--source` nor `--source-file` was provided.  
**Fix:** Pass the local path or remote SAS URL via `--source`, or put the URL in a file and use `--source-file <path>` (useful when SAS tokens contain `&` characters that shell may interpret).

#### `source file '<path>' is empty`
**Cause:** The file passed to `--source-file` exists but contains no content.  
**Fix:** Check that the file contains the correct source URL.

#### `model version already exists`  
Also shown as: `✗ Model '<name>' version '<version>' already exists.`  
**Cause:** A model with the same name and version is already registered.  
**Fix:** Use a different `--version` value. Run `azd ai models show --name <name>` to see what versions exist.

#### `✗ Permission denied: you do not have the required role to upload custom models.`
**Cause:** Your identity lacks the required RBAC role on the Foundry project.  
**Fix:** Ensure you have the appropriate role. See:
- [Prerequisites for custom model import](https://learn.microsoft.com/en-us/azure/foundry/how-to/fireworks/import-custom-models?tabs=rest-api#prerequisites)
- [RBAC in Azure AI Foundry](https://learn.microsoft.com/en-us/azure/foundry/concepts/rbac-foundry)

#### `--lora-* flags are only valid when --weight-type is LoRA`
**Cause:** You passed `--lora-rank`, `--lora-alpha`, `--lora-target-modules`, or `--lora-dropout` without setting `--weight-type LoRA`.  
**Fix:** Add `--weight-type LoRA` to the command, or remove the LoRA-specific flags.

#### `--lora-rank is required and must be a positive integer when --weight-type is LoRA`  
#### `--lora-alpha is required and must be a positive integer when --weight-type is LoRA`
**Cause:** You used `--weight-type LoRA` but omitted the required LoRA configuration flags.  
**Fix:** Provide both `--lora-rank <N>` and `--lora-alpha <N>` where N is a positive integer.

#### `--lora-target-modules contains an empty entry`
**Cause:** The comma-separated list passed to `--lora-target-modules` contains an empty segment (e.g. `q_proj,,v_proj`).  
**Fix:** Remove the extra comma.

#### `registration failed: <error>`
**Cause:** The backend rejected the model registration request after upload completed.  
**Fix:** Check the wrapped error message for details (e.g. invalid schema, quota limit, unsupported model format).

#### `upload failed: <error>`  
#### `azcopy failed: <error>`
**Cause:** The file transfer to the staging blob storage failed.  
**Fix:** Check network connectivity. If using a SAS URL via `--source`, verify it hasn't expired. Retry; transient blob storage errors are common.

#### `unexpected response: no blob reference returned`
**Cause:** The API accepted the pending-upload request but returned an empty blob reference. This is a backend-side issue.  
**Fix:** Retry. If it persists, file a support request.

---

### Model Show/List/Update/Delete

#### `model '<name>' not found`
**Cause:** No model with the given name exists in the project.  
**Fix:** Run `azd ai models list` to see available models.

#### `--remove-tag value cannot be empty`
**Cause:** `--remove-tag` was passed with an empty string.  
**Fix:** Provide a non-empty tag key.

#### `invalid tag format "<value>": expected key=value`
**Cause:** A `--tag` value doesn't follow the `key=value` format.  
**Fix:** Use `--tag mykey=myvalue`.

---

### azcopy (shared by both extensions)

#### `no azcopy download available for <OS>/<arch>`
**Cause:** The auto-installer doesn't support your OS/architecture combination.  
**Fix:** Manually download `azcopy` from [aka.ms/downloadazcopy](https://aka.ms/downloadazcopy) and pass the path via `--azcopy-path`.

#### `azcopy not found at specified path: <path>`
**Cause:** The binary pointed to by `--azcopy-path` doesn't exist at that location.  
**Fix:** Verify the path is correct and the binary is executable.
