import re
with open('bsimvis/app/services/similarity_service.py', 'r') as f:
    content = f.read()
content = content.replace('total_sims = written or 0\n                \n            if job_service', 'total_sims = written or 0\n            else:\n                total_sims = 0\n                \n            if job_service')
with open('bsimvis/app/services/similarity_service.py', 'w') as f:
    f.write(content)
