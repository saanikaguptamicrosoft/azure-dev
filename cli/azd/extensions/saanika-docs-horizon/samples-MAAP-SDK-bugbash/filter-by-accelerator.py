from azure.identity import DefaultAzureCredential 
from azure.mgmt.cognitiveservices import CognitiveServicesManagementClient 

  

credential = DefaultAzureCredential() 
client = CognitiveServicesManagementClient(credential, "46de64dd-dcd9-4fa6-ac61-3cc4ffc2a7e0") 

caps = client.managed_compute_capacities.list(
    offer="GlobalManagedCompute", 
    accelerator_type="H100_80GB",    
)

for cap in caps: 
    print(f"{cap.properties.accelerator_type}: " 
    f"{cap.properties.available_accelerators} GPUs") 