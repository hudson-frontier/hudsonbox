// Generated from api/openapi.json. Do not edit.
import type { Schema } from "./wire.js";
export const schema: Schema = {
  "models": {
    "RequestedResources": {
      "fields": {
        "vcpu": {
          "type": {
            "kind": "integer",
            "wide": false,
            "min": "-2147483648",
            "max": "2147483647"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        },
        "memory_mib": {
          "type": {
            "kind": "integer",
            "wide": true,
            "min": "-9223372036854775808",
            "max": "9223372036854775807"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        },
        "disk_mib": {
          "type": {
            "kind": "integer",
            "wide": true,
            "min": "-9223372036854775808",
            "max": "9223372036854775807"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        }
      },
      "closed": false
    },
    "CreateRequest": {
      "fields": {
        "image_digest": {
          "type": {
            "kind": "string"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        },
        "name": {
          "type": {
            "kind": "nullable",
            "inner": {
              "kind": "string"
            }
          },
          "required": false,
          "nullable": true,
          "omit_false": false
        },
        "resources": {
          "type": {
            "kind": "ref",
            "name": "RequestedResources"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        },
        "correlation_id": {
          "type": {
            "kind": "nullable",
            "inner": {
              "kind": "string"
            }
          },
          "required": false,
          "nullable": true,
          "omit_false": false
        }
      },
      "closed": false
    },
    "CommandInput": {
      "fields": {
        "argv": {
          "type": {
            "kind": "array",
            "inner": {
              "kind": "string"
            }
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        },
        "env": {
          "type": {
            "kind": "map",
            "inner": {
              "kind": "string"
            }
          },
          "required": false,
          "nullable": false,
          "omit_false": false,
          "default": {}
        },
        "cwd": {
          "type": {
            "kind": "string"
          },
          "required": false,
          "nullable": false,
          "omit_false": false,
          "default": "/"
        },
        "deadline_unix_ms": {
          "type": {
            "kind": "integer",
            "wide": true,
            "min": "-9223372036854775808",
            "max": "9223372036854775807"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        },
        "output_limit": {
          "type": {
            "kind": "integer",
            "wide": true,
            "min": "0",
            "max": "18446744073709551615"
          },
          "required": false,
          "nullable": false,
          "omit_false": false,
          "default": 1048576
        },
        "expected_generation": {
          "type": {
            "kind": "integer",
            "wide": true,
            "min": "-9223372036854775808",
            "max": "9223372036854775807"
          },
          "required": false,
          "nullable": false,
          "omit_false": false,
          "reject_null": true
        }
      },
      "closed": true
    },
    "DestroyRequest": {
      "fields": {
        "correlation_id": {
          "type": {
            "kind": "nullable",
            "inner": {
              "kind": "string"
            }
          },
          "required": false,
          "nullable": true,
          "omit_false": false
        }
      },
      "closed": true
    },
    "CancelRequest": {
      "fields": {},
      "closed": true
    },
    "AdmittedResponse": {
      "fields": {
        "sandbox_id": {
          "type": {
            "kind": "string"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        },
        "operation_id": {
          "type": {
            "kind": "string"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        },
        "status": {
          "type": {
            "kind": "string"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        },
        "status_url": {
          "type": {
            "kind": "string"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        }
      },
      "closed": false
    },
    "ProblemBody": {
      "fields": {
        "operation_id": {
          "type": {
            "kind": "string"
          },
          "required": false,
          "nullable": false,
          "omit_false": false
        },
        "title": {
          "type": {
            "kind": "string"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        },
        "status": {
          "type": {
            "kind": "integer",
            "wide": false,
            "min": "0",
            "max": "65535"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        },
        "code": {
          "type": {
            "kind": "string"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        }
      },
      "closed": false
    },
    "OperationBody": {
      "fields": {
        "response_expired": {
          "type": {
            "kind": "boolean"
          },
          "required": false,
          "nullable": false,
          "omit_false": true,
          "default": false
        },
        "operation_id": {
          "type": {
            "kind": "string"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        },
        "sandbox_id": {
          "type": {
            "kind": "string"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        },
        "target_operation_id": {
          "type": {
            "kind": "string"
          },
          "required": false,
          "nullable": false,
          "omit_false": false
        },
        "kind": {
          "type": {
            "kind": "string"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        },
        "status": {
          "type": {
            "kind": "string"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        },
        "phase": {
          "type": {
            "kind": "string"
          },
          "required": false,
          "nullable": false,
          "omit_false": false
        },
        "output_status": {
          "type": {
            "kind": "string"
          },
          "required": false,
          "nullable": false,
          "omit_false": false
        },
        "result": {
          "type": {
            "kind": "json"
          },
          "required": false,
          "nullable": false,
          "omit_false": false
        },
        "error": {
          "type": {
            "kind": "json"
          },
          "required": false,
          "nullable": false,
          "omit_false": false
        },
        "created_at": {
          "type": {
            "kind": "string"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        },
        "completed_at": {
          "type": {
            "kind": "string"
          },
          "required": false,
          "nullable": false,
          "omit_false": false
        }
      },
      "closed": false
    },
    "SandboxBody": {
      "fields": {
        "observation_simulated": {
          "type": {
            "kind": "boolean"
          },
          "required": false,
          "nullable": false,
          "omit_false": false
        },
        "sandbox_id": {
          "type": {
            "kind": "string"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        },
        "name": {
          "type": {
            "kind": "string"
          },
          "required": false,
          "nullable": false,
          "omit_false": false
        },
        "desired_state": {
          "type": {
            "kind": "string"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        },
        "observed_state": {
          "type": {
            "kind": "string"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        },
        "observed_at": {
          "type": {
            "kind": "string"
          },
          "required": false,
          "nullable": false,
          "omit_false": false
        },
        "image_digest": {
          "type": {
            "kind": "string"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        },
        "resources": {
          "type": {
            "kind": "json"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        },
        "generation": {
          "type": {
            "kind": "integer",
            "wide": true,
            "min": "-9223372036854775808",
            "max": "9223372036854775807"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        },
        "active_operation_id": {
          "type": {
            "kind": "string"
          },
          "required": false,
          "nullable": false,
          "omit_false": false
        },
        "created_at": {
          "type": {
            "kind": "string"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        }
      },
      "closed": false
    },
    "SandboxList": {
      "fields": {
        "items": {
          "type": {
            "kind": "array",
            "inner": {
              "kind": "ref",
              "name": "SandboxBody"
            }
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        },
        "next_cursor": {
          "type": {
            "kind": "nullable",
            "inner": {
              "kind": "string"
            }
          },
          "required": true,
          "nullable": true,
          "omit_false": false
        }
      },
      "closed": false
    },
    "OperationList": {
      "fields": {
        "items": {
          "type": {
            "kind": "array",
            "inner": {
              "kind": "ref",
              "name": "OperationBody"
            }
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        },
        "next_cursor": {
          "type": {
            "kind": "nullable",
            "inner": {
              "kind": "string"
            }
          },
          "required": true,
          "nullable": true,
          "omit_false": false
        }
      },
      "closed": false
    },
    "FileCaptureRequest": {
      "fields": {
        "path": {
          "type": {
            "kind": "string"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        }
      },
      "closed": true
    },
    "FileCaptureResponse": {
      "fields": {
        "capture": {
          "type": {
            "kind": "string"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        },
        "size": {
          "type": {
            "kind": "integer",
            "wide": true,
            "min": "0",
            "max": "18446744073709551615"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        },
        "sha256": {
          "type": {
            "kind": "string"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        },
        "expires_unix_ms": {
          "type": {
            "kind": "integer",
            "wide": true,
            "min": "-9223372036854775808",
            "max": "9223372036854775807"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        },
        "chunk_size": {
          "type": {
            "kind": "integer",
            "wide": false,
            "min": "0",
            "max": "4294967295"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        },
        "simulated": {
          "type": {
            "kind": "boolean"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        },
        "guest_reported": {
          "type": {
            "kind": "boolean"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        }
      },
      "closed": false
    },
    "OutputStats": {
      "fields": {
        "seen": {
          "type": {
            "kind": "integer",
            "wide": true,
            "min": "0",
            "max": "18446744073709551615"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        },
        "stored": {
          "type": {
            "kind": "integer",
            "wide": true,
            "min": "0",
            "max": "18446744073709551615"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        },
        "truncated": {
          "type": {
            "kind": "boolean"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        }
      },
      "closed": false
    },
    "OutputEvent": {
      "fields": {
        "stream": {
          "type": {
            "kind": "string"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        },
        "offset": {
          "type": {
            "kind": "integer",
            "wide": true,
            "min": "0",
            "max": "18446744073709551615"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        },
        "next_offset": {
          "type": {
            "kind": "integer",
            "wide": true,
            "min": "0",
            "max": "18446744073709551615"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        },
        "data_base64": {
          "type": {
            "kind": "string"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        },
        "at_end": {
          "type": {
            "kind": "boolean"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        },
        "complete": {
          "type": {
            "kind": "boolean"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        },
        "seen": {
          "type": {
            "kind": "integer",
            "wide": true,
            "min": "0",
            "max": "18446744073709551615"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        },
        "stored": {
          "type": {
            "kind": "integer",
            "wide": true,
            "min": "0",
            "max": "18446744073709551615"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        },
        "truncated": {
          "type": {
            "kind": "boolean"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        },
        "simulated": {
          "type": {
            "kind": "boolean"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        },
        "guest_reported": {
          "type": {
            "kind": "boolean"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        }
      },
      "closed": false
    },
    "EndEvent": {
      "fields": {
        "reason": {
          "type": {
            "kind": "string"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        },
        "stdout": {
          "type": {
            "kind": "ref",
            "name": "OutputStats"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        },
        "stderr": {
          "type": {
            "kind": "ref",
            "name": "OutputStats"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        },
        "simulated": {
          "type": {
            "kind": "boolean"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        },
        "guest_reported": {
          "type": {
            "kind": "boolean"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        }
      },
      "closed": false
    },
    "StreamProblemEvent": {
      "fields": {
        "code": {
          "type": {
            "kind": "string"
          },
          "required": true,
          "nullable": false,
          "omit_false": false
        }
      },
      "closed": false
    }
  },
  "codes": [
    "bad_request",
    "payload_too_large",
    "unauthenticated",
    "forbidden",
    "image_not_allowed",
    "not_found",
    "gone",
    "response_expired",
    "output_not_ready",
    "output_expired",
    "output_missing",
    "output_corrupt",
    "output_range_invalid",
    "file_capture_missing",
    "file_response_invalid",
    "file_range_invalid",
    "command_in_progress",
    "execution_capacity_exhausted",
    "sandbox_generation_mismatch",
    "conflict",
    "unavailable",
    "internal"
  ],
  "version": "0.0.1"
};
