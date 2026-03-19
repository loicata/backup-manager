# CLAUDE.md — Directives pour Claude Code

Ce fichier définit les règles de travail que Claude doit respecter dans ce projet.

---

## 🌐 Langue

- **Code, commentaires, noms de variables, messages de logs, UI** : toujours en **anglais**.
- **Communication avec l'utilisateur** (réponses, questions, explications) : toujours en **français**.

---

## 🧠 Principes généraux

- Toujours comprendre l'intention avant d'agir. En cas de doute, poser une question courte avant de coder.
- Favoriser la lisibilité et la maintenabilité sur la concision.
- Chaque modification doit être cohérente avec l'architecture existante du projet.
- Ne jamais supprimer du code existant sans l'avoir explicitement signalé et justifié.
- Signaler toute dette technique introduite avec un commentaire `# TODO:` ou `# TECH-DEBT:`.

---

## ✅ Tests — règle absolue

**Tout code produit ou modifié doit être accompagné de tests.**

### Règles de test systématiques

1. **Nouvelle fonction → nouveau test unitaire** sans exception.
2. **Bug corrigé → test de non-régression** qui aurait détecté le bug.
3. **Module modifié → vérifier que les tests existants passent toujours.**
4. **Avant de considérer une tâche terminée**, exécuter la suite de tests complète.

### Structure des tests

```
tests/
├── unit/          # Tests unitaires par module
├── integration/   # Tests d'intégration entre composants
└── fixtures/      # Données de test partagées
```

### Commandes de test

```bash
# Lancer tous les tests
pytest

# Lancer avec couverture
pytest --cov=. --cov-report=term-missing

# Lancer un fichier spécifique
pytest tests/unit/test_mon_module.py -v
```

### Seuil de couverture cible : **≥ 80 %**

---

## 📁 Structure du projet

```
.
├── CLAUDE.md          ← ce fichier
├── README.md
├── src/               ← code source principal
├── tests/             ← tous les tests
├── docs/              ← documentation
├── scripts/           ← scripts utilitaires
└── config/            ← fichiers de configuration
```

---

## 🔧 Conventions de code

### Python

- Style : **PEP 8** + formatage **Black** (`black .`)
- Linting : **flake8** ou **ruff**
- Type hints obligatoires sur toutes les fonctions publiques
- Docstrings au format **Google Style**

```python
def ma_fonction(param: str) -> bool:
    """Fait quelque chose d'utile.

    Args:
        param: Description du paramètre.

    Returns:
        True si succès, False sinon.

    Raises:
        ValueError: Si param est vide.
    """
```

### Nommage

| Élément      | Convention       | Exemple              |
|--------------|------------------|----------------------|
| Variables    | snake_case       | `nb_clients`         |
| Fonctions    | snake_case       | `get_status()`       |
| Classes      | PascalCase       | `NetworkScanner`     |
| Constantes   | UPPER_SNAKE_CASE | `MAX_RETRY`          |
| Fichiers     | snake_case       | `network_utils.py`   |

---

## 🔐 Sécurité (priorité haute pour ce projet)

- **Ne jamais écrire de secrets en dur** (clés, mots de passe, tokens).
- Utiliser des variables d'environnement ou un fichier `.env` (non commité).
- Valider et assainir toutes les entrées externes.
- Logger les événements de sécurité sans y inclure de données sensibles.
- Toute fonction exposée sur le réseau doit avoir une gestion d'erreur explicite.

---

## 📝 Gestion des commits (si applicable)

Format de message de commit :

```
type(scope): description courte

Corps optionnel expliquant le pourquoi.
```

Types : `feat`, `fix`, `refactor`, `test`, `docs`, `chore`, `security`

Exemples :
```
feat(scanner): ajouter la détection des ports UDP
fix(api): corriger le timeout sur les connexions lentes
test(scanner): couvrir le cas d'hôte inaccessible
```

---

## 🏗️ Qualité de code — approche lente et rigoureuse

**La vitesse n'est pas un objectif. La fiabilité l'est.**
Claude doit prendre le temps nécessaire pour produire du code correct du premier coup.

### Avant d'écrire la moindre ligne

- Reformuler mentalement la tâche pour s'assurer de l'avoir bien comprise.
- Identifier les cas limites et les scénarios d'erreur **avant** de coder.
- Vérifier si une fonction similaire existe déjà dans le projet pour éviter les doublons.
- Choisir la solution la plus simple qui résout le problème (pas la plus élégante).

### Programmation défensive — règles obligatoires

- **Valider toutes les entrées** en début de fonction (type, plage, valeur nulle).
- **Ne jamais faire confiance** aux données externes (API, fichiers, réseau, utilisateur).
- **Toujours gérer les cas d'erreur explicitement** — pas de `except: pass`, pas d'erreur silencieuse.
- **Utiliser des assertions** pour documenter les invariants internes du code.
- Préférer **échouer tôt et bruyamment** plutôt que de continuer dans un état incohérent.

```python
# ✅ Bon exemple
def connect(host: str, port: int) -> socket.socket:
    if not host or not isinstance(host, str):
        raise ValueError(f"Invalid host: {host!r}")
    if not (1 <= port <= 65535):
        raise ValueError(f"Port out of range: {port}")
    # ...

# ❌ Mauvais exemple
def connect(host, port):
    # on tente directement, on verra bien
    return socket.connect((host, port))
```

### Auto-relecture obligatoire avant de rendre le code

Après avoir écrit une fonction ou un module, Claude doit **relire son propre code** en se posant ces questions :

1. Cette fonction fait-elle **une seule chose** ?
2. Que se passe-t-il si un argument est `None`, vide, ou hors plage ?
3. Que se passe-t-il si le réseau est indisponible / le fichier manquant / la DB inaccessible ?
4. Y a-t-il des **ressources à fermer** (fichiers, connexions, sockets) en cas d'erreur ?
5. Les messages d'erreur sont-ils **clairs et actionnables** pour le développeur ?
6. Le code est-il **lisible sans commentaires** ? Sinon, ajouter des commentaires.
7. Y a-t-il des **magic numbers** à remplacer par des constantes nommées ?

### Gestion des erreurs — standard du projet

```python
# Structure attendue pour tout bloc potentiellement défaillant
try:
    result = operation_risquee()
except SpecificException as e:
    logger.error("Context of failure: %s", e)
    raise  # ou retourner une valeur de fallback explicite
finally:
    cleanup()  # toujours libérer les ressources
```

- Capturer les exceptions **les plus spécifiques possible**, jamais `Exception` seule sauf en dernier recours.
- Logger avec le contexte suffisant pour diagnostiquer sans relancer le programme.
- Ne jamais laisser une exception se propager silencieusement.

### Complexité et lisibilité

- Une fonction ne doit pas dépasser **30 lignes** (hors docstring et assertions).
- Pas plus de **3 niveaux d'indentation** — extraire en sous-fonctions sinon.
- Toute condition complexe (`if a and b or not c`) doit être nommée dans une variable intermédiaire.

```python
# ✅
is_valid_target = host is not None and port > 0 and not is_blacklisted(host)
if is_valid_target:
    ...

# ❌
if host is not None and port > 0 and not is_blacklisted(host):
    ...
```

### Logging — obligatoire sur les points critiques

- Entrée/sortie de toute fonction réseau ou système de fichiers.
- Chaque branche d'erreur doit avoir un `logger.warning()` ou `logger.error()`.
- Niveau de log cohérent : `DEBUG` pour le détail, `INFO` pour les événements normaux, `WARNING` pour les anomalies récupérables, `ERROR` pour les échecs.

---

## 🚫 Ce que Claude ne doit PAS faire

- Ne pas modifier des fichiers hors du scope de la tâche demandée.
- Ne pas introduire de nouvelles dépendances sans les signaler.
- Ne pas ignorer les erreurs avec des `except: pass` silencieux.
- Ne pas laisser du code de debug (`print()`, `breakpoint()`) dans le code final.
- Ne pas créer de fichiers temporaires sans les nettoyer.

---

## 📋 Checklist avant de rendre une tâche

- [ ] Le code compile / s'exécute sans erreur
- [ ] Les tests unitaires couvrent le nouveau code
- [ ] Les tests existants passent toujours
- [ ] Pas de secrets ou données sensibles dans le code
- [ ] Docstrings présentes sur les fonctions publiques
- [ ] Pas de `print()` de debug oubliés
- [ ] Les imports sont propres (pas d'imports inutilisés)

---

*Ce fichier est lu automatiquement par Claude Code à chaque session dans ce projet.*
