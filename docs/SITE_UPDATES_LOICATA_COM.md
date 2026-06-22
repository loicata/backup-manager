# loicata.com — Contenus prêts à intégrer (WordPress)

*Textes en anglais, prêts à coller. Instructions en français.*

---

## A. Corrections techniques SEO (admin WordPress)

1. **Permaliens** : Réglages → Permaliens → « Titre de publication » (supprime `/index.php/`). Vérifier que les 301 sont automatiques.
2. **Canonical** : la home pointe vers `http://www.loicata.com/` — forcer `https` dans le plugin SEO (Yoast/Rank Math) et dans Réglages → Général (URL du site en https).
3. **Title de la home** : `Home - loicata` → :

```
Free Open-Source Cybersecurity for Microbusinesses | loicata
```

4. **Meta description de la home** :

```
Free, open-source cybersecurity tools for microbusinesses: ransomware-proof backup with immutable S3 storage, autonomous network monitoring. No subscription, no account.
```

---

## B. Nouvelle page : `/backup-manager/`

Créer une page dédiée (Pages → Ajouter) et la lier depuis Products.

**Title SEO :**
```
Backup Manager — Free Ransomware-Proof Backup for Windows (S3 Object Lock)
```

**Meta description :**
```
Free, open-source Windows backup with immutable S3 Object Lock storage. Your backups become impossible to delete — even by ransomware. No AWS knowledge needed.
```

**Contenu de la page :**

```markdown
# Backup Manager — Ransomware-Proof Backup for Windows

**Free and open-source. Your backups become physically impossible to delete — even by ransomware with administrator rights.**

[Download for Windows 10 / 11](https://github.com/loicata/backup-manager/releases/latest)

## Why it's different

Ransomware doesn't just encrypt your files — it deletes your backups first. Backup Manager stores your backups on Amazon S3 with **Object Lock in Compliance mode**, the same WORM (write-once-read-many) technology banks use for regulatory archives. Once written, nobody can delete them before the retention date you chose. Not ransomware. Not a stolen admin account. Not even you.

This is not behavioral detection that malware can bypass. It is a physical impossibility, enforced by AWS itself.

## No cloud expertise required

The 11-step wizard creates your AWS account bucket, applies the lock, estimates the cost before you commit, and schedules everything: monthly full backups, daily differentials, SHA-256 integrity checks, email alerts. AWS bills you directly (typically a few dollars a month) — Backup Manager itself is free, with no account and no subscription.

## How it compares

- **Veeam Agent Free** cannot back up to the cloud at all — immutability is reserved for paid editions.
- **Acronis True Image** requires a subscription, and its ransomware protection is behavioral detection, not immutability.
- **Duplicati, restic, and Kopia** are excellent free tools, but none of them supports S3 Object Lock.

Backup Manager is the only free, open-source Windows backup application with native Object Lock immutability.

## Also a complete classic backup tool

External drives (with hardware-serial detection), network shares, SFTP servers, and any S3-compatible storage (Wasabi, Backblaze B2, Scaleway, OVH, Cloudflare R2…). AES-256-GCM streaming encryption, up to two independent mirror copies, GFS retention, scheduling, and email reports.

**Open source (GPL v3) — 2,781 automated tests, 87% coverage.**
[Source code on GitHub](https://github.com/loicata/backup-manager)
```

**JSON-LD à ajouter sur la page** (bloc HTML personnalisé) :

```html
<script type="application/ld+json">
{
  "@context": "https://schema.org",
  "@type": "SoftwareApplication",
  "name": "Backup Manager",
  "operatingSystem": "Windows 10, Windows 11",
  "applicationCategory": "SecurityApplication",
  "offers": { "@type": "Offer", "price": "0", "priceCurrency": "USD" },
  "license": "https://www.gnu.org/licenses/gpl-3.0.html",
  "url": "https://www.loicata.com/backup-manager/",
  "downloadUrl": "https://github.com/loicata/backup-manager/releases/latest",
  "author": { "@type": "Person", "name": "Loic Ader" }
}
</script>
```

---

## C. Page Products — remplacer le paragraphe Backup Manager

Texte actuel (obsolète — ne mentionne pas l'anti-ransomware) :

> A reliable, secure, and user-friendly Windows backup application designed for personal and small-business use. Manage multiple backup profiles, store copies on local drives, network shares, or remote servers, and let the built-in scheduler and GFS retention policy take care of the rest.

**Nouveau texte :**

```
Free, open-source ransomware-proof backup for Windows. Backup Manager stores
your backups on Amazon S3 with Object Lock — the immutable WORM technology
used in banking — making them impossible to delete, even by ransomware with
admin rights. The guided wizard sets up everything: no AWS knowledge needed,
no subscription, no account. Classic mode also covers external drives,
network shares, SFTP and S3-compatible storage, with AES-256-GCM encryption
and GFS retention.

→ Learn more: /backup-manager/   → Source: github.com/loicata/backup-manager
```

---

## D. GitHub (2 minutes, fort impact SEO)

1. **Topics du dépôt** (Settings → Topics) : `backup`, `windows`, `ransomware-protection`, `immutable-backups`, `s3-object-lock`, `anti-ransomware`, `aes-256`, `open-source`, `python`.
2. **Description du dépôt** :

```
Free, open-source ransomware-proof backup for Windows. Immutable S3 Object Lock backups that even ransomware cannot delete. No AWS knowledge needed.
```

---

## E. Idées d'articles (requêtes « alternative », faible concurrence)

1. *Veeam Agent Free can't back up to the cloud — here's a free alternative with immutable backups*
2. *Why Duplicati, restic and Kopia can't do S3 Object Lock (and what to use instead)*
3. *Ransomware deletes backups first: how Object Lock Compliance mode stops it*
