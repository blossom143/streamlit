# AI-Powered SQL Query Assistant - Tsomorlig's Version

## Supporting Services

1. Create a postgres database on [render.com](https://dashboard.render.com/)
2. [Buy tokens](https://platform.openai.com/settings/organization/billing/overview)
3. [Generate a API key](https://platform.openai.com/settings/organization/api-keys)
   

## Steps to run

1. Checkout repo `git clone`
2. Rename `sample.env` to `.env` and fill in the information
3. Create a new python environment `python -mvenv .venv`
4. Activate environment `source .venv\bin\activate`
5. Install packages `pip install -r requirements.txt`
6. Generate password `python generate_password.py`
7. Run database test `python test_render_database.py`
8. Populate database `python populate_db.py`
9. Run Streamlit app `streamlit run streamlit_app.py`


## How create hashed password

```python
import bcrypt
password = "some_strong_password".encode('utf-8')
hashed = bcrypt.hashpw(password, bcrypt.gensalt())
print(hashed.decode())
```