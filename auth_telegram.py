"""
Telegram Network Mapper - Authentification Telethon
===================================================
Lance ce script UNE SEULE FOIS pour te connecter à ton compte Telegram.
Il va te demander ton numéro de téléphone puis un code de vérification.
Ensuite, un fichier de session sera créé et l'app web pourra l'utiliser
automatiquement sans redemander le code.

Usage:
    python auth_telegram.py
"""

import asyncio
import sys
import os

try:
    from config import TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_PHONE
except ImportError:
    print("Erreur: config.py introuvable. Crée-le d'abord avec tes identifiants API.")
    sys.exit(1)

from telethon import TelegramClient

SESSION_FILE = os.path.join(os.path.dirname(__file__), 'telegram_mapper_session')


async def main():
    print("=" * 50)
    print("  Authentification Telegram (Telethon)")
    print("=" * 50)
    print()
    print(f"  API ID:   {TELEGRAM_API_ID}")
    print(f"  API Hash: {TELEGRAM_API_HASH[:8]}...")
    print()

    phone = TELEGRAM_PHONE
    if not phone:
        phone = input("  Ton numéro de téléphone (format international, ex: +33612345678): ").strip()

    print()
    print("  Connexion à Telegram...")

    client = TelegramClient(SESSION_FILE, TELEGRAM_API_ID, TELEGRAM_API_HASH)

    await client.start(phone=phone)

    me = await client.get_me()
    print()
    print(f"  Connecté en tant que : {me.first_name} {me.last_name or ''} (@{me.username or 'N/A'})")
    print(f"  Session sauvegardée dans : {SESSION_FILE}.session")
    print()
    print("  Tu peux maintenant utiliser le mode Telethon dans l'app web !")
    print("  Lance: python app.py")

    await client.disconnect()


if __name__ == '__main__':
    asyncio.run(main())
