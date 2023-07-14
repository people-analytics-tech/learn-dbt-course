lint or pre-commit:
	poetry run pre-commit run -a

requirements:
	poetry export -f requirements.txt --output requirements.txt --without-hashes
