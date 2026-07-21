# Plan — détection des flux de capital par réconciliation de balance

Garantir qu'un dépôt/retrait non rapporté par le broker (IG démo en tête,
mais généralisable) ne soit jamais lu comme du rendement. Défense ultime
anti-phantom-cashflow, broker-agnostique.

## 1. Problème & preuve

L'enclave dérive `Deposits`/`Withdrawals` d'un snapshot **uniquement** de
`GetCashflows` — ce que le broker déclare ([sync.go:563](../internal/service/sync.go#L563)),
plus l'inception-deposit au 1ᵉʳ snapshot ([sync.go:612](../internal/service/sync.go#L612)).
Aucune inférence par balance.

Le probe IG a prouvé le trou : un « add funds » de 500 € sur compte démo
n'apparaît dans **aucune** API (transactions tous `type`, activity,
accounts). Il bouge la balance mais reste invisible au ledger. Sur un
broker qui cache les dépôts, la balance monte → lu comme rendement →
track record gonflable (un user qui dépose 1-10 $/jour fabrique une perf).

Le probe a aussi montré que la **détection existe déjà dans les chiffres** :
`implied opening = settled − Σ(P&L ledger)` est passé de 10000,00 →
10500,00 exactement au dépôt. Le flux invisible ressort au centime.

## 2. Principe

**La balance settled fait foi, pas le broker.** Toute variation de la
balance réalisée que les trades + frais n'expliquent pas EST un flux de
capital.

    flux_capital(t) = Δsettled(t) − [ Σ pnl_réalisé(t) + Σ frais(t) ]

- `settled` = `RealizedBalance` = `Equity − UnrealizedPnL` (déjà stocké ;
  exclut le non-réalisé, donc les positions ouvertes ne polluent pas).
- `pnl_réalisé` = Σ `Trade.RealizedPnL` sur la fenêtre.
- `frais` = Σ `GetFundingFees` sur la fenêtre (funding/intérêt/commission
  non nettés dans un deal ; signés négatifs).

## 3. Méthode normalisée (le composant réutilisable)

Un helper pur, sans dépendance broker :

    InferCapitalFlow(prev, cur ReconInputs) (deposit, withdrawal float64)

    ReconInputs{
        SettledBalance float64   // RealizedBalance du snapshot
        RealizedPnL    float64   // Σ Trade.RealizedPnL de la fenêtre
        Charges        float64   // Σ FundingFees de la fenêtre (signé)
        ReportedFlow   float64   // Σ GetCashflows de la fenêtre (signé)
    }

Calcul :

    inferred = cur.Settled − prev.Settled − cur.RealizedPnL − cur.Charges
    residual = inferred − cur.ReportedFlow           // la part que le broker n'a PAS expliquée
    total    = cur.ReportedFlow + gate(residual)     // gate = 0 sous le seuil

- `total > 0` → `deposit = total`, `withdrawal = 0` ; sinon l'inverse.
- Sur un broker qui rapporte bien : `residual ≈ 0` → `total = ReportedFlow`
  (comportement actuel inchangé, l'inférence n'est qu'un **cross-check**).
- Sur IG démo (dépôt caché) : `ReportedFlow = 0`, `residual = le dépôt` →
  capté. C'est le « gap deposit ».

Le broker `GetCashflows` sert désormais à **attribuer/dater** la part
connue ; la balance fournit le total. On ne fait plus confiance à personne.

## 4. Contrat connecteur & éligibilité

Le composant ne marche que si `pnl_réalisé + frais` est **complet** : un
frais manquant fait apparaître un faux retrait (risque inverse). C'est
exactement ce que le gate du probe valide (`implied opening ≈ 0` sur la vie
du compte).

Éligibilité gardée, comme `externalRebuilderExchanges` :

    var balanceReconciledExchanges = []string{ /* "ig", … */ }

Un exchange n'entre dans la liste qu'après avoir passé le gate de
complétude via `cmd/ig-probe` (ou l'équivalent) : résidu ≈ 0 hors dépôt.
Ça évite d'activer l'inférence sur un connecteur qui sous-déclare ses frais
et générerait des faux flux.

Pré-requis connecteur (déjà satisfaits par IG) :
- `RealizedBalance` propre (equity − unrealized).
- `Trade.RealizedPnL` fiable par trade.
- `GetFundingFees` capturant TOUTES les charges hors-deal (cf. le fix
  WITH/cashTransaction récent).

## 5. Points d'intégration

- Snapshot live : `syncConnection` ([sync.go:561-614](../internal/service/sync.go#L561))
  et le chemin daily ([sync.go:1004](../internal/service/sync.go#L1004)).
- Snapshot précédent : `GetLatestByUserExchangeLabel(user, exchange, label)`
  ([snapshot.go:613](../internal/repository/snapshot.go#L613)).
- Insérer APRÈS le calcul de `deposits/withdrawals` reportés (étape 6) et de
  `RealizedBalance`, AVANT la création du snapshot (étape 9). Remplacer
  `deposits/withdrawals` par la sortie de `InferCapitalFlow` quand
  l'exchange est dans `balanceReconciledExchanges`.
- `RealizedPnL` de la fenêtre : sommer les `trades` déjà fetchés (étape 4).

## 6. Gardes & cas limites

- **Seuil** : `max(seuil_abs, seuil_rel × equity)` (ex. max(0,50 €,
  0,05 %)). Absorbe le bruit d'arrondi/FX. Le probe IG réconcilie au
  centime → un seuil bas suffit et détecte les petits dépôts (1-10 $).
  La résolution de détection = précision de `pnl_réalisé + frais`.
- **1ᵉʳ snapshot** : pas de `prev` → on garde l'inception-deposit existant
  ([sync.go:612](../internal/service/sync.go#L612)). L'inférence démarre au 2ᵉ.
- **Direction** : residual > seuil → dépôt ; < −seuil → retrait.
- **Attribution** : au jour du snapshot courant (même sémantique que la
  fenêtre 24 h actuelle).
- **Double-compte** : garanti nul car `total = reported + residual` et
  `residual = inferred − reported`.
- **Non-réalisé isolé** : usage de `settled`, pas `equity` — une position
  qui bouge en non-réalisé n'émet aucun flux ; à sa clôture le P&L passe
  en `RealizedPnL` → expliqué.
- **Multi-devises** : les charges FX déjà converties par le broker sont
  dans le ledger → expliquées. Résidu FX résiduel absorbé par le seuil.

## 7. Tests

- **Unitaires** (helper pur, table-driven) : dépôt caché (reported=0,
  Δsettled>0 → dépôt) ; dépôt reporté (reported=Δ → residual≈0, pas de
  gap) ; retrait ; bruit sous seuil → 0 ; frais manquant simulé → montre
  pourquoi l'éligibilité est gardée.
- **Séquence e2e** (mock IG) : snapshot J, injecter un top-up invisible
  (balance +X sans ligne ledger), snapshot J+1 → assert dépôt X détecté,
  jamais lu comme rendement.
- **Live IG** : déposer un montant connu sur le démo, 2 syncs, confirmer
  que le flux est capté et non compté en performance.

## 8. Déploiement

1. Helper + tests unitaires (isolé, sans risque).
2. Câblage dans les 2 chemins de sync, derrière `balanceReconciledExchanges`
   vide au départ (no-op).
3. Passer le gate de complétude IG (probe), ajouter `"ig"` à la liste.
4. Fast-path enclave (Go-only, mesure inchangée).
5. Valider live (dépôt démo connu → capté).

## 9. Normalisation / autres brokers

Le helper est **broker-agnostique** : tout connecteur exposant settled +
P&L réalisé + frais y est éligible, une fois le gate de complétude passé.
Candidats à évaluer ensuite (GetCashflows non fiable ou dépôts démo cachés) :
les suspects du scan phantom-cashflow — bitget / mt5 / ibkr — cf.
`binance-phantom-cashflow-perimeter-gap`. Chacun s'active individuellement
après son propre passage au gate, jamais en masse.
