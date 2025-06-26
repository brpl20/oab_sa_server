#!/bin/bash

# Script de Inicialização do Servidor OAB SA
# Autor: Configuração automática para ambiente de web scraping
# Data: $(date)

set -e  # Sair em caso de erro

# Cores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Função para log
log() {
    echo -e "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')] ✓${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[$(date '+%Y-%m-%d %H:%M:%S')] ⚠${NC} $1"
}

log_error() {
    echo -e "${RED}[$(date '+%Y-%m-%d %H:%M:%S')] ✗${NC} $1"
}

# Verificar se está rodando como usuário correto (não root)
if [ "$EUID" -eq 0 ]; then
    log_error "Este script não deve ser executado como root. Execute como usuário normal."
    exit 1
fi

# Verificar se arquivo .env existe na home do usuário
ENV_FILE="$HOME/.env"
if [ ! -f "$ENV_FILE" ]; then
    log_error "Arquivo .env não encontrado em $ENV_FILE"
    log "Crie o arquivo .env na sua home com as seguintes variáveis:"
    echo "AWS_ACCESS_KEY_ID=sua_access_key"
    echo "AWS_SECRET_ACCESS_KEY=sua_secret_key"
    echo "AWS_DEFAULT_REGION=us-east-1"
    echo "AWS_BUCKET=oab-jsons-sa2"
    echo "GITHUB_TOKEN=seu_github_token"
    exit 1
fi

# Carregar variáveis do .env
source "$ENV_FILE"

log "🚀 Iniciando configuração do servidor..."

# 1. Atualizar sistema
log "📦 Atualizando sistema..."
sudo apt update && sudo apt upgrade -y
log_success "Sistema atualizado"

# 2. Instalar dependências básicas
log "📦 Instalando dependências básicas..."
sudo apt install -y curl wget unzip software-properties-common apt-transport-https ca-certificates gnupg lsb-release python3-pip python3-venv git
log_success "Dependências básicas instaladas"

# 3. Instalar AWS CLI
log "☁️ Instalando AWS CLI..."
if ! command -v aws &> /dev/null; then
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
    unzip awscliv2.zip
    sudo ./aws/install
    rm -rf awscliv2.zip aws/
    log_success "AWS CLI instalado"
else
    log_warning "AWS CLI já está instalado"
fi

# 4. Configurar AWS CLI
log "⚙️ Configurando AWS CLI..."
mkdir -p ~/.aws
cat > ~/.aws/credentials << EOF
[default]
aws_access_key_id = $AWS_ACCESS_KEY_ID
aws_secret_access_key = $AWS_SECRET_ACCESS_KEY
EOF

cat > ~/.aws/config << EOF
[default]
region = $AWS_DEFAULT_REGION
output = json
EOF

log_success "AWS CLI configurado"

# 5. Instalar Google Chrome
log "🌐 Instalando Google Chrome..."
if ! command -v google-chrome &> /dev/null; then
    wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | sudo apt-key add -
    echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" | sudo tee /etc/apt/sources.list.d/google-chrome.list
    sudo apt update
    sudo apt install -y google-chrome-stable
    log_success "Google Chrome instalado"
else
    log_warning "Google Chrome já está instalado"
fi

# 6. Instalar Firefox
log "🦊 Instalando Firefox..."
if ! command -v firefox &> /dev/null; then
    sudo apt install -y firefox
    log_success "Firefox instalado"
else
    log_warning "Firefox já está instalado"
fi

# 7. Instalar GitHub CLI
log "🐙 Instalando GitHub CLI..."
if ! command -v gh &> /dev/null; then
    curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg | sudo dd of=/usr/share/keyrings/githubcli-archive-keyring.gpg
    sudo chmod go+r /usr/share/keyrings/githubcli-archive-keyring.gpg
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" | sudo tee /etc/apt/sources.list.d/github-cli.list > /dev/null
    sudo apt update
    sudo apt install -y gh
    log_success "GitHub CLI instalado"
else
    log_warning "GitHub CLI já está instalado"
fi

# 8. Configurar GitHub CLI
log "⚙️ Configurando GitHub CLI..."
echo "$GITHUB_TOKEN" | gh auth login --with-token
log_success "GitHub CLI configurado"

# 9. Clonar repositório
log "📥 Clonando repositório..."
REPO_DIR="$HOME/oab_sa_server"
if [ ! -d "$REPO_DIR" ]; then
    cd "$HOME"
    gh repo clone brpl20/oab_sa_server
    log_success "Repositório clonado em $REPO_DIR"
else
    log_warning "Repositório já existe em $REPO_DIR"
    cd "$REPO_DIR"
    git pull
    log_success "Repositório atualizado"
fi

# 10. Entrar na pasta do repositório
cd "$REPO_DIR"
log "📂 Entrando na pasta do repositório: $REPO_DIR"

# 11. Criar ambiente virtual Python
log "🐍 Criando ambiente virtual Python..."
ENV_NAME="oab_sa_server_env"
if [ ! -d "$ENV_NAME" ]; then
    python3 -m venv "$ENV_NAME"
    log_success "Ambiente virtual '$ENV_NAME' criado"
else
    log_warning "Ambiente virtual '$ENV_NAME' já existe"
fi

# 12. Ativar ambiente virtual e instalar requirements
log "📦 Instalando dependências Python..."
source "$ENV_NAME/bin/activate"
if [ -f "requirements.txt" ]; then
    pip install --upgrade pip
    pip install -r requirements.txt
    log_success "Dependências Python instaladas"
else
    log_warning "Arquivo requirements.txt não encontrado"
fi

# 13. Copiar arquivo .env para pasta do projeto
log "📋 Copiando arquivo .env para o projeto..."
cp "$ENV_FILE" "$REPO_DIR/.env"
log_success "Arquivo .env copiado para o projeto"

# 14. Baixar arquivo específico do S3
for i in {101..125}; do
    S3_SOURCE="s3://$AWS_BUCKET/input/lawyers_${i}.json"
    LOCAL_DEST="./lawyers_${i}.json"
    
    log "📥 Baixando lawyers_${i}.json..."
    DOWNLOAD_TOTAL=$((DOWNLOAD_TOTAL + 1))
    
    if aws s3 cp "$S3_SOURCE" "$LOCAL_DEST"; then
        log_success "✓ lawyers_${i}.json baixado com sucesso"
        DOWNLOAD_SUCCESS=$((DOWNLOAD_SUCCESS + 1))
    else
        log_warning "⚠ Falha ao baixar lawyers_${i}.json (arquivo pode não existir)"
    fi
done


# 15. Verificar instalações
log "🔍 Verificando instalações..."
echo "Versões instaladas:"
echo "- AWS CLI: $(aws --version)"
echo "- Google Chrome: $(google-chrome --version)"
echo "- Firefox: $(firefox --version)"
echo "- GitHub CLI: $(gh --version)"
echo "- Python: $(python3 --version)"

log_success "🎉 Configuração do servidor concluída!"
log "📁 Diretório do projeto: $REPO_DIR"
log "🐍 Para ativar o ambiente virtual: source $REPO_DIR/$ENV_NAME/bin/activate"
log "▶️ Para executar o projeto, navegue até $REPO_DIR e ative o ambiente virtual"

echo ""
echo "=== PRÓXIMOS PASSOS ==="
echo "1. cd $REPO_DIR"
echo "2. source $ENV_NAME/bin/activate"
echo "3. python seu_script.py"
echo ""

