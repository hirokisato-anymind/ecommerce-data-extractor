.PHONY: backend frontend dev install

install:
	cd backend && pip install -e .
	cd frontend && pnpm install

backend:
	cd backend && uvicorn app.main:app --reload --port 8000

frontend:
	cd frontend && pnpm dev

dev:
	@echo "Run in separate terminals:"
	@echo "  make backend"
	@echo "  make frontend"
