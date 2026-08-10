# Configuration
$NifiContainerName = "nifi"
$CurrentDir = Get-Location
$BackupDir = Join-Path $CurrentDir "nifi_backups"
$Timestamp = Get-Date -Format "yyyyMMdd_HHmmss"

# Créer le répertoire de sauvegarde s'il n'existe pas
if (-not (Test-Path $BackupDir)) {
    New-Item -ItemType Directory -Path $BackupDir | Out-Null
}

# Fonction pour vérifier si le conteneur NiFi est en cours d'exécution
function Check-Container {
    $containerStatus = docker ps --filter "name=$NifiContainerName" --format "{{.Names}}"
    if (-not $containerStatus) {
        Write-Host "Erreur : Le conteneur $NifiContainerName n'est pas en cours d'exécution."
        exit 1
    }
}

# Fonction pour sauvegarder flow.json.gz
function Backup-FlowJson {
    Write-Host "Sauvegarde de flow.json.gz..."
    $backupFile = Join-Path $BackupDir "flow.json.gz.$Timestamp"
    docker cp "$($NifiContainerName):/opt/nifi/nifi-current/conf/flow.json.gz" $backupFile
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Sauvegarde de flow.json.gz réussie : $backupFile"
    } else {
        Write-Host "Erreur lors de la sauvegarde de flow.json.gz"
        exit 1
    }
}

# Fonction pour charger flow.json.gz
function Load-FlowJson {
    param (
        [string]$FlowJsonFile
    )
    Write-Host "Chargement de $FlowJsonFile..."
    # Vérifier si le conteneur est en cours d'exécution
    $containerStatus = docker ps --filter "name=$NifiContainerName" --format "{{.Names}}"
    if (-not $containerStatus) {
        Write-Host "Le conteneur $NifiContainerName n'est pas en cours d'exécution. Démarrage temporaire pour ajuster les permissions..."
        docker start $NifiContainerName
        Start-Sleep -Seconds 5  # Attendre que le conteneur démarre
    }
    # Copier le fichier
    docker cp $FlowJsonFile "$($NifiContainerName):/opt/nifi/nifi-current/conf/flow.json.gz"
    # Ajuster les permissions en tant que root
    Write-Host "Ajustement des permissions pour flow.json.gz..."
    docker exec --user root $NifiContainerName bash -c "chown nifi:nifi /opt/nifi/nifi-current/conf/flow.json.gz && chmod 644 /opt/nifi/nifi-current/conf/flow.json.gz"
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Erreur lors de l'ajustement des permissions"
        exit 1
    }
    # Arrêter le conteneur
    Write-Host "Arrêt du conteneur pour finaliser le chargement..."
    docker stop $NifiContainerName
    # Redémarrer le conteneur
    docker start $NifiContainerName
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Chargement de $FlowJsonFile réussi"
    } else {
        Write-Host "Erreur lors du chargement de $FlowJsonFile"
        exit 1
    }
}

# Menu principal
Write-Host "Script de sauvegarde et chargement pour flow.json.gz de NiFi"
Write-Host "1. Sauvegarder flow.json.gz"
Write-Host "2. Charger flow.json.gz"
Write-Host "3. Quitter"
$choice = Read-Host "Choisis une option (1-3)"

switch ($choice) {
    1 {
        Check-Container
        Backup-FlowJson
    }
    2 {
        $flowJsonFile = Read-Host "Entrez le chemin du fichier flow.json.gz à charger (ex: $BackupDir\flow.json.gz.20250404_123456)"
        if (-not (Test-Path $flowJsonFile)) {
            Write-Host "Erreur : Fichier $flowJsonFile non trouvé"
            exit 1
        }
        Load-FlowJson -FlowJsonFile $flowJsonFile
    }
    3 {
        Write-Host "Au revoir !"
        exit 0
    }
    default {
        Write-Host "Option invalide"
        exit 1
    }
}