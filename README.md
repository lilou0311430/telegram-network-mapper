# Telegram Network Mapper

Outil de cartographie des réseaux de chaînes Telegram par exploration en profondeur (BFS).
Conçu pour l'analyse OSINT des écosystèmes informationnels sur Telegram.

## Fonctionnalités

- **Crawl multi-niveaux** : exploration BFS à profondeur configurable depuis des chaînes "seed"
- **Deux méthodes de scraping** :
  - **Web scraping** (sans compte) : scrape les previews publiques t.me
  - **Telethon API** (avec compte) : accès complet via l'API Telegram
- **Détection de liens** : forwards, mentions (@), liens t.me, références dans les descriptions
- **Métadonnées** : titre, abonnés, description, statut vérifié
- **Visualisation interactive** : graphe D3.js avec zoom, drag, filtres, recherche
- **Export** : GEXF (Gephi), GraphML, JSON

## Installation

```bash
cd telegram-network-mapper
pip install -r requirements.txt
```

## Lancement

```bash
python app.py
```

Ouvrir http://localhost:5000 dans le navigateur.

## Utilisation

1. Entrer une ou plusieurs chaînes seed dans le champ texte
2. Configurer la profondeur (2-3 recommandé pour commencer)
3. Choisir la méthode (web scraping ou Telethon)
4. Lancer le crawl
5. Observer le graphe se construire en temps réel
6. Exporter en GEXF pour analyse avancée dans Gephi

## Utilisation avec Telethon (optionnel)

Pour utiliser l'API Telegram directement :

1. Aller sur https://my.telegram.org
2. Créer une application pour obtenir `api_id` et `api_hash`
3. Sélectionner "Telethon API" dans l'interface
4. Entrer les identifiants

## Structure

```
telegram-network-mapper/
├── app.py              # Serveur Flask + API
├── crawler.py          # Moteur de crawl BFS
├── scraper.py          # Modules de scraping (Web + Telethon)
├── requirements.txt    # Dépendances Python
├── templates/
│   └── index.html      # Interface web (D3.js)
└── README.md
```

## Notes

- Le web scraping est limité aux chaînes **publiques**
- Respecter un délai entre les requêtes (1.5s par défaut) pour éviter le rate limiting
- Pour les grands graphes (>200 nœuds), privilégier l'export Gephi pour la visualisation
