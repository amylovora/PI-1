Pi-1 Project

Start mysql db
docker compose up -d

Stop mysql db
docker compose stop

Install python dependencies
pip install --no-cache-dir --upgrade -r ./requirements.txt

Run Python Api
cd python
uvicorn --host 127.0.0.1 main:app --reload
