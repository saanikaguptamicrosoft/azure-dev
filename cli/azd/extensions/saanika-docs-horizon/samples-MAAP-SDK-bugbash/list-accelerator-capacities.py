from azure.identity import DefaultAzureCredential 
from azure.mgmt.cognitiveservices import CognitiveServicesManagementClient 

  

credential = DefaultAzureCredential() 
client = CognitiveServicesManagementClient(credential, "46de64dd-dcd9-4fa6-ac61-3cc4ffc2a7e0") 
caps = client.managed_compute_capacities.list(offer="GlobalManagedCompute") 

for cap in caps: 
    print(f"{cap.properties.accelerator_type} ({cap.properties.location}): " 
        f"{cap.properties.available_accelerators} GPUs") 
    for ds in cap.properties.deployment_size_capacities: 
        print(f"  {ds.model_instance_accelerator_count} accel/instance: " 
            f"total={ds.total_available_capacity}, " 
            f"largest_block={ds.largest_deployment_capacity}") 