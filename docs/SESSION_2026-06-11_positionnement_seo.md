# Session du 11 juin 2026 — Positionnement, SEO, site et incident GitHub

## Objectif initial

Travailler le positionnement de Backup Manager face à ses concurrents et son référencement (marché anglophone), avec mise à jour du README et de loicata.com.

## Réalisé

### 1. Analyse concurrentielle (`docs/POSITIONING.md`)

Positionnement retenu : **seul logiciel de backup Windows gratuit et open-source avec immutabilité native S3 Object Lock (mode Compliance), sans connaissance AWS requise**. Vérifié contre Veeam Agent Free (pas de cible cloud en gratuit), Acronis (abonnement, détection comportementale), Macrium (Free abandonné), Duplicati/restic/Kopia (pas d'Object Lock — conflits architecturaux documentés). Faiblesse assumée : pas d'image disque / bare-metal.

### 2. README (commits `f637ab0` + `13ea50a`, poussés)

Accroche SEO en tête + section « How it compares » avec tableau face aux 6 concurrents. Incident : le commit fait depuis l'environnement Cowork a tronqué le fichier (vue périmée du montage) — réparé par Claude Code, qui a aussi actualisé les chiffres (2781 tests, 87 %). **Règle adoptée : ne plus commiter depuis Cowork ; éditer les fichiers et laisser Claude Code/l'utilisateur commiter.**

### 3. loicata.com (WordPress)

- Page `/backup-manager/` : accroche, section comparative, title/meta Yoast optimisés, focus keyphrase « ransomware proof backup », JSON-LD SoftwareApplication, chiffres 2781/87 %.
- Page `/products/` : paragraphe de positionnement + title/meta.
- Home : title « Free Open-Source Cybersecurity for Microbusinesses | loicata » + meta. Canonical passé en https.
- **Permaliens** : `/index.php/` supprimé (structure « Post name »), redirections 301 vérifiées, liens internes et JSON-LD corrigés.
- **Images** : migrées de raw.githubusercontent vers la médiathèque WordPress (`/wp-content/uploads/2026/06/`) — correctif permanent, à conserver.

### 4. Google Search Console

Propriété `https://www.loicata.com/` créée, vérifiée (meta tag via Yoast → Site connections), sitemap `sitemap_index.xml` soumis. Statut « Couldn't fetch » initial normal — à revérifier sous 2-3 jours.

### 5. GitHub

- Description du dépôt réécrite (ransomware-proof, Object Lock) ; topics ajoutés : `ransomware-protection`, `immutable-backups`, `s3-object-lock`, `anti-ransomware`, `open-source` ; `proton-drive` retiré.
- Licence vérifiée : **GPL v3 partout** (LICENSE, pyproject, README, License.rtf de l'installeur, détection GitHub). Aucune trace MIT hors historique.

## ⚠️ Incident en cours : compte GitHub flaggé

Le 11/06 en cours de journée, le dépôt public est devenu **invisible pour les visiteurs non connectés** (404 sur page, raw, releases) — flag anti-spam automatique confirmé par GitHub (« Your account has been flagged »). Suspicion : mots-clés « ransomware » ajoutés à la description/topics (faux positif). Le MSI fonctionne pour le propriétaire connecté uniquement.

- Vérification SMS passée, **ticket de réinstallation #4473623** déposé (accusé de réception reçu).
- Décision : attendre la réponse du support (pas de contournement déployé).
- Contournement prêt si besoin : héberger le MSI sur loicata.com et rediriger les boutons Download.

## À suivre

1. Réponse GitHub sur le ticket #4473623 → vérifier le dépôt en navigation privée une fois levé.
2. Search Console : statut du sitemap sous 2-3 jours, premières données d'indexation.
3. Les modifications de `docs/POSITIONING.md` et `docs/SITE_UPDATES_LOICATA_COM.md` (chiffres 2781/87 %) sont en attente de commit côté Windows.
4. Idées d'articles SEO (faible concurrence) listées dans `docs/SITE_UPDATES_LOICATA_COM.md` §E.
