#!/usr/bin/env python3
import json
import gzip
import os

def compile_flow():
    # File path bindings
    input_path = 'conf/nifi/NiFi_Flow.json'
    output_json_path = 'conf/nifi/flow.json'
    output_gz_path = 'conf/nifi/flow.json.gz'

    if not os.path.exists(input_path):
        print(f"Error: Ingress source '{input_path}' not found!")
        return

    print(f"Reading Process Group definition from: {input_path}")
    with open(input_path, 'r', encoding='utf-8') as f:
        pg_flow = json.load(f)

    # Translate the Process Group JSON into a Full Server Dataflow Configuration
    # Mapping the process group 'flowContents' directly to the server's 'rootGroup'
    server_flow = {
        "flowEncodingVersion": pg_flow.get("flowEncodingVersion", "1.0"),
        "parameterContexts": [],
        "parameterProviders": [],
        "controllerServices": [],
        "reportingTasks": [],
        "templates": [],
        "rootGroup": pg_flow.get("flowContents", {})
    }

    # Convert parameterContexts from Map (NiFi 2.x export format) to List (expected by the server JVM)
    param_contexts = pg_flow.get("parameterContexts", {})
    if isinstance(param_contexts, dict):
        server_flow["parameterContexts"] = list(param_contexts.values())
    elif isinstance(param_contexts, list):
        server_flow["parameterContexts"] = param_contexts

    print(f"Writing server-level flow configuration to: {output_json_path}")
    with open(output_json_path, 'w', encoding='utf-8') as f:
        json.dump(server_flow, f, indent=2)

    print(f"Compressing configuration to: {output_gz_path}")
    with open(output_json_path, 'rb') as f_in:
        with gzip.open(output_gz_path, 'wb') as f_out:
            f_out.writelines(f_in)

    print("--------------------------------------------------")
    print("NiFi Flow compiled successfully! Ready for mounting.")
    print("--------------------------------------------------")

if __name__ == '__main__':
    compile_flow()
