from azure.identity import DefaultAzureCredential 
from azure.mgmt.cognitiveservices import CognitiveServicesManagementClient 

credential = DefaultAzureCredential() 
client = CognitiveServicesManagementClient(credential, "46de64dd-dcd9-4fa6-ac61-3cc4ffc2a7e0") 
usages = client.managed_compute_usages_operation_group.list(location="westus2") 
for u in usages: 
    remaining = u.limit - u.current_value 
    print(f"{u.name.value}: {u.current_value}/{u.limit} {u.unit} " 
        f"(remaining: {remaining}, scope: {u.offer_scope})") 