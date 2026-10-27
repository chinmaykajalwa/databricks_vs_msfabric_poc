# ---------------------------------------------------------------------------
# Generator VM variables
# ---------------------------------------------------------------------------

variable "create_vm" {
  description = "Provision the generator VM. Set false to keep only Event Hubs + storage (run the generator elsewhere)."
  type        = bool
  default     = true
}

variable "vm_size" {
  description = "B2ms (2 vCPU / 8 GB) runs lite in ~2-3 min and baseline (~2.8 GB peak) comfortably. B2s (4 GB) is enough for lite only."
  type        = string
  default     = "Standard_B2ms"
}

variable "os_disk_size_gb" {
  description = "OS disk. Lite output is ~350 MB, baseline a few GB."
  type        = number
  default     = 64
}

variable "admin_username" {
  description = "Admin username for SSH."
  type        = string
  default     = "azureuser"
}

variable "ssh_source_cidr" {
  description = "CIDR allowed to SSH (port 22). Your own IP /32 -- find it with: curl -s https://api.ipify.org"
  type        = string

  validation {
    condition     = var.ssh_source_cidr != "0.0.0.0/0" && can(cidrhost(var.ssh_source_cidr, 0))
    error_message = "ssh_source_cidr must be a valid CIDR scoped to your own IP (e.g. 203.0.113.7/32), never 0.0.0.0/0."
  }
}

# ---- Repository ------------------------------------------------------------

variable "repo_url" {
  description = "HTTPS URL of the GitHub repo the VM clones (this repo). Must contain no secrets."
  type        = string
}

variable "repo_ref" {
  description = "Branch, tag or commit SHA to deploy. Pin a tag or SHA for the demo day so the VM runs exactly what CI tested."
  type        = string
  default     = "main"
}

variable "repo_is_private" {
  description = "True if the repo needs authentication. Makes a missing PAT a plan-time error instead of a VM that boots, fails to clone, and reports success."
  type        = bool
  default     = true
}

variable "git_pat" {
  description = "GitHub fine-grained PAT, Contents: Read on this one repo. Supply via TF_VAR_git_pat, never a tfvars file. Revoke at teardown."
  type        = string
  sensitive   = true
  default     = ""
}

check "git_pat_present_for_private_repo" {
  assert {
    condition     = !var.create_vm || !var.repo_is_private || length(var.git_pat) > 0
    error_message = <<-EOT
      repo_is_private = true but git_pat is empty. The VM would fail to clone and run nothing.
        export TF_VAR_git_pat="github_pat_xxxxxxxx"     # bash
        $env:TF_VAR_git_pat = "github_pat_xxxxxxxx"     # PowerShell
      If the repository is public, set repo_is_private = false.
    EOT
  }
}

variable "generator_subdir" {
  description = "Path of the generator package inside the repo."
  type        = string
  default     = "generator"
}

# ---- What the VM does on first boot ------------------------------------------

variable "data_profile" {
  description = "Dataset profile: lite (fast, the demo default) or baseline (~10x entities; needs >= 8 GB RAM)."
  type        = string
  default     = "lite"

  validation {
    condition     = contains(["lite", "baseline"], var.data_profile)
    error_message = "data_profile must be lite or baseline."
  }
}

variable "run_pipeline_on_boot" {
  description = "Run steps 1-4 + validation + the dry-run gate on first boot."
  type        = bool
  default     = true
}

variable "upload_to_storage" {
  description = "Upload the generated data to the landing / groundtruth containers after the pipeline passes."
  type        = bool
  default     = true
}

variable "publisher_autostart" {
  description = "Start the live publisher automatically after setup. Default false: the 60-minute story is meant to be started when the audience is watching (sudo systemctl start p2p-publisher)."
  type        = bool
  default     = false
}

variable "publisher_duration_min" {
  description = "Live run length in plant minutes. The story events fire at minutes 5, 15, 20, 30 and 45."
  type        = number
  default     = 60
}

variable "publisher_time_scale" {
  description = "Plant seconds per wall second. 1.0 = real time (the demo); 6.0 = the 60-minute story in 10 minutes (rehearsal)."
  type        = number
  default     = 1.0
}

variable "publisher_flush_ms" {
  description = "Plant-time flush window: 5000 = ~5 s latency (default); 60000 = ~8x fewer billable events, up to 60 s latency."
  type        = number
  default     = 5000
}
