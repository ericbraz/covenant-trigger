from pathlib import Path

# API path
API_URL = "http://localhost:3002"

# Brazil prefix number
BRAZIL_PREFIX = "+55"

# Maximum attempts to request information from user on terminal: recommended = 5
MAX_ATTEMPTS = 5

# Minimum interval between messages sent through Whatsapp: recommended = 30 s
MIN_INTERVAL = 30

# Max messages sent every time the code runs: max recommended = 50
MAX_TRIGGERS = 30

# Number used for all time.sleep across the code as an interval to display info on terminal
GLOBAL_TIMER = 2

# Different variants in each message sent
VARIANTS = [
    "O que não te agrada no seu site?",
    "Seu site está atendendo todas as suas expectativas?",
    "Cansado de sites que parecem todos iguais?",
    "Seu site não está trazendo os resultados que você espera?",
    "Você está conseguindo diferenciar seu site da concorrência?",
]

DIVIDER = "____________________________________________________________________________________"

def get_project_root():
    """
    Attempts to determine the project root directory using Pathlib.

    Returns:
        Path object representing the project root directory, or None if not found.
    """
    current_dir = Path(__file__)
    while current_dir != current_dir.parent:
        if (current_dir / ".git").exists() or (current_dir / ".gitignore").exists() or (current_dir / ".env").exists() or (current_dir / "setup.py").exists():
            return current_dir
        current_dir = current_dir.parent
    print("testing this shit")
    return None

PROJECT_ROOT = get_project_root()

if not PROJECT_ROOT:
    print("Error: Could not determine project root directory.")

def path(relative_path):
    return "{}{}".format(PROJECT_ROOT, relative_path)
