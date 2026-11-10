# Workshop demo: banca e shop multiutente

Piano per un'app dimostrativa che gira su un server vero, con più partecipanti collegati
dal telefono e uno schermo grande che mostra cosa succede nel sistema mentre succede.
Usa solo `effect-cqrs` ed Effect v4.

## 1. Requisiti, chiariti

### Partecipanti

- **R1 Ingresso.** Un partecipante entra con un click e sceglie un
  nickname. Il nickname è un dato personale.
- **R2 Conto.** All'ingresso gli si apre un conto con saldo zero. L'id del conto è l'id
  utente nel token (`sub` del JWT), quindi ognuno ha un conto e può comandare solo il suo.
- **R3 Ricarica.** "Carica soldi" accredita denaro finto, con un massimo per ricarica (100) e
  per minuto, così nessuno gonfia il sistema.
- **R4 Bonifico.** Si sceglie un altro partecipante dalla lista dei presenti e gli si manda
  un importo. Se il destinatario ha chiuso il conto nel frattempo, la saga rimborsa il
  mittente.
- **R5 Shop.** Un catalogo condiviso con stock reale: due persone che comprano l'ultimo
  pezzo nello stesso istante non lo ottengono entrambe. Si paga con il saldo del conto.
- **R6 Backorder.** Se un prodotto è esaurito si può ordinarlo lo stesso. I soldi vengono
  addebitati subito, l'ordine entra in una coda per prodotto e viene evaso in ordine di
  arrivo quando arriva merce. Finché è in coda si può annullare e i soldi tornano indietro.
- **R7 Chiusura e oblio.** "Chiudi conto" annulla i backorder aperti, paga il saldo
  residuo rimborsi compresi, chiude il conto, distrugge la chiave dei dati personali (crypto-shredding con
  `PersonalData.forget`) e da lì il token non vale più. Gli eventi restano nel
  log con importi e date, mentre nickname e indirizzo si leggono come `[forgotten]`.
- **R8 Dati personali.** Nickname e indirizzo di spedizione (facoltativo, chiesto al primo
  ordine) sono campi `PersonalData.field` con il partecipante come subject.

### Bot

- **R9 Rifornimento.** Un bot nel server guarda lo stock ogni N secondi e rifornisce i
  prodotti sotto soglia. Agisce con comandi normali, firmati come attore `bot`, quindi
  compare nel log come chiunque altro.
- **R10 Controllo del bot.** Dalla sala di controllo si può mettere in pausa, cambiare
  intervallo e soglia, e forzare un rifornimento subito. È il modo per far vedere
  i backorder che si evadono.

### Sala di controllo (schermo del presentatore)

- **R11 Presenze.** Chi entra e chi esce in tempo reale: connessioni aperte, conti aperti e
  chiusi, con il nickname finché esiste.
- **R12 Flusso eventi.** Gli eventi del log man mano che vengono scritti, con aggregate,
  versione, posizione, attore e causazione: chi ha causato cosa, comprese le saghe.
- **R13 Dati.** Lo stato dei view model: conti e saldi, catalogo e stock, ordini e code di
  backorder, saghe in corso.
- **R14 Metriche.** Comandi al secondo e latenza per esito, eventi scritti, ritardo dei view
  model rispetto alla testa del log, connessioni SSE, saghe aperte e completate. Mostra
  anche un invariante: la somma dei saldi più gli incassi dello shop è uguale al totale
  ricaricato meno il totale prelevato.
- **R15 Trace.** Le span del server (comando, append, saga, view model) in diretta,
  riusando `examples/devtools/TraceView.tsx`.
- **R16 Dato personale prima e dopo.** Per un conto chiuso, l'evento `AccountOpened` com'era
  e come si legge ora.
- **R17 Bottoni del presentatore.** Pausa, ripresa e velocità del bot; rifornimento
  immediato; simulatore di clienti on/off (genera traffico se in sala siete in pochi);
  "rompi il prossimo pagamento" per mostrare la compensazione; ricostruzione di un view
  model da zero; reset della demo.

### API e documentazione

- **R18 REST.** `HttpApi` di Effect con OpenAPI, Swagger UI (`HttpApiSwagger`) su `/docs` e
  `/openapi.json`. La UI usa lo stesso `api.ts` con `HttpApiClient`, quindi è tipata.
- **R19 Stream.** Server-Sent Events con `effect/encoding/Sse`: un canale personale (il mio
  conto e i miei ordini), uno pubblico (eventi, con i dati personali solo come
  `[personal]`) e uno per la sala di controllo (presenze, metriche, span).
- **R20 AsyncAPI.** `EventCatalog.document` genera il documento AsyncAPI 3 degli eventi di
  dominio; i canali di presenze e metriche si aggiungono a mano. È servito su
  `/asyncapi.json` e mostrato in una pagina della UI.

### Fuori ambito

Valute vere, pagamenti veri, email, notifiche push, più istanze del server.

## 2. Decisioni

| Tema | Scelta | Perché |
|---|---|---|
| Codice | `examples/workshop/` in questo repo: `domain/`, `server/`, `ui/`, `api.ts` condiviso | Usa il framework dai sorgenti, entra in `pnpm check`, un solo repo GitHub |
| Backend | Container Node su **Render free** (Dockerfile), deploy automatico da GitHub | Gratuito e senza carta; saghe, bot e SSE vogliono un processo sempre acceso |
| Piano B | Lo stesso container sul portatile dietro `cloudflared tunnel --url` | Gratuito, nessun limite di risorse, pronto se Render ha problemi |
| UI | Vite + React + Tailwind, statica su **Vercel Hobby** dallo stesso repo | Come `bank-ui`; gratuita |
| Database | **Supabase free**, connessione dal **session pooler** (porta 5432) | Il transaction pooler non regge `LISTEN`; la connessione diretta è solo IPv6 |
| Auth | Il server emette i suoi token: JWT HS256 firmati con WebCrypto e `AUTH_SECRET`, validi un giorno | Un click da telefono, nessuna password, nessun servizio esterno; funziona uguale in locale, su Render e col tunnel |
| Admin | Chi entra con il codice `ADMIN_CODE` riceve un token con ruolo `admin` | Semplice, nessuna lista di id da configurare |
| Saghe e checkpoint | `WorkflowEngine.layerMemory` e checkpoint in memoria; all'avvio tutto si ricostruisce dal log | È l'abbinamento che `Saga.ts` raccomanda senza cluster; serve che ogni comando di saga sia idempotente, e lo è per `orderId`/`transferId` |
| Dati personali | `PersonalData.layerSql` su Postgres, master key da variabile d'ambiente | Le chiavi sopravvivono ai riavvii |

## 3. Dominio

Il conto parte da `examples/bank-ui/src/domain/account.ts`, lo shop dal tutorial
`examples/tutorial/shop.ts`. Le differenze sono queste.

**`account`**
- Comandi: `openAccount { nickname }`, `deposit`, `sendTransfer`, `receiveTransfer`,
  `refundTransfer`, `payOrder { orderId, amount }`, `refundOrder { orderId, amount,
  reason }`, `closeAccount`.
- `payOrder` e `refundOrder` ricordano gli `orderId` già visti, così una saga che ripete
  il passo non addebita due volte.
- `closeAccount` emette `MoneyWithdrawn` del residuo e `AccountClosed`.
- Il conto non sa quali ordini sono ancora in coda, quindi la chiusura passa da
  `POST /me/close`: annulla i backorder aperti del partecipante e li rimborsa nella
  stessa richiesta, poi esegue `closeAccount` e `PersonalData.forget`.

**`product`**
- Comandi: `list { name, emoji, price, stock }`, `restock { quantity }`,
  `take { orderId, quantity }`, `enqueue { orderId, quantity }`, `dequeue { orderId }`.
- Lo stato tiene la disponibilità e la coda FIFO dei backorder. `restock` ed `enqueue`
  assegnano subito la merce disponibile alla coda, in ordine, con un
  `BackorderAllocated` per ordine. Il primo ordine che non ci sta ferma l'assegnazione.
- Con una coda, `take` rifiuta: chi arriva dopo si mette in fila.

**`order`**
- Comandi: `place`, `markPaid`, `markBackordered`, `fulfil`, `cancel`, `reject { reason }`.
  Tranne `place` e `cancel`, non fanno niente se l'ordine ha già passato quel punto.
- Stati: `placed → paid → fulfilled`, oppure `paid → backordered → fulfilled | cancelled`,
  oppure `rejected` (saldo insufficiente, conto chiuso o esaurito senza backorder).
- L'indirizzo di spedizione di R8 è rimandato: il nickname basta come dato personale.

**Saghe**
- `Transfer`: `TransferSent` porta a `receiveTransfer` sul destinatario; se fallisce,
  `refundTransfer`.
- `Checkout`: `OrderPlaced` porta a `payOrder`, poi `markPaid` e `take`. Se c'è merce,
  `fulfil`; altrimenti, con il backorder, `enqueue` e `markBackordered`, senza,
  `refundOrder` e `reject`. Se il pagamento fallisce, `reject`.
- `Fulfil`: `BackorderAllocated` porta a `fulfil` dell'ordine.
- `RefundCancelled`: `OrderCancelled` porta a `refundOrder`.
- La chiusura chiama `PersonalData.forget` subito dopo `closeAccount`, nella stessa
  richiesta: non serve una saga.

**View model**: `Participants` (conti, nickname, saldi, presenze), `Catalog` (prodotti,
stock, coda), `Orders` (per cliente e globali), `Ledger` (movimenti per conto),
`Totals` (l'invariante di R14).

## 4. Architettura

```
 telefoni ──┐
            ├─ UI statica (Vercel)
 proiettore ┘        │  REST + SSE           Supabase Postgres (session pooler)
                     ▼                              ▲
        Render: un processo Node                    │ LISTEN/NOTIFY, eventi, chiavi
        ├─ HttpApi: REST, /docs, /openapi.json, /asyncapi.json
        ├─ middleware: CORS, token del server → CurrentUser, rate limit
        ├─ AggregateService: account, product, order
        ├─ Saga: Transfer, Checkout, Fulfil, RefundCancelled
        ├─ ViewModel: Participants, Catalog, Orders, Ledger, Totals
        ├─ Bot e simulatore di clienti (fiber con Schedule)
        └─ Hub: PubSub di eventi, presenze, metriche, span → SSE
```

`EventSource` non manda header, quindi gli stream SSE si aprono con `fetch` e il parser
di `effect/encoding/Sse` nella UI, che può mandare `Authorization: Bearer`. Ogni stream
manda un heartbeat ogni 15 secondi, per i proxy e per non far dormire Render.

## 5. Fasi

Ogni fase finisce con `pnpm check` e `pnpm test` verdi e con il deploy aggiornato.

1. **Scheletro in produzione.** *Fatto in locale; deploy da collegare.* Cartelle, `api.ts` con `/health`, server con
   `PostgresEventStore`, Swagger, CORS, Dockerfile, script `pnpm workshop` e
   `pnpm ui:workshop`. Progetto Supabase, servizio Render e progetto Vercel collegati
   a GitHub.
   *Fatto quando* la UI su Vercel mostra la risposta di `/health` dal server su Render,
   che è collegato a Supabase.
2. **Auth.** *Fatta.* `/auth/join` con nickname e codice admin facoltativo; token
   firmati dal server; middleware `HttpApiSecurity.bearer` che verifica il token e il
   conto e fornisce `CurrentUser`. Non esiste un endpoint che prenda l'id di un altro
   conto: ognuno comanda solo il proprio.
   *Fatto quando* una richiesta senza token, con un token alterato o di un conto chiuso
   riceve `401`.
3. **Banca.** *Fatta.* Aggregate `account`, saga `Transfer`, view model `Participants` e
   `Ledger`, endpoint, test in `test/Workshop.test.ts`, chiusura con oblio del nickname.
   *Fatto quando* due browser aprono un conto, ricaricano e si scambiano soldi, e un
   bonifico a un conto chiuso torna indietro.
4. **Shop e backorder.** *Fatta*, tranne `Totals`, rimandato alla sala di controllo.
   Aggregate `product` e `order`, saghe `Checkout`, `Fulfil` e `RefundCancelled`, view
   model `Catalog` e `Orders`, rifornimento manuale per l'admin, chiusura del conto che
   annulla e rimborsa i backorder. Il catalogo è di prova: caffè, cornetto, succo,
   cioccolato, quaderno, cappellino e cuffie, queste ultime esaurite in partenza.
   *Fatto quando* un test lancia 20 acquisti concorrenti sull'ultimo pezzo e ne passa uno
   solo, e un backorder viene evaso al rifornimento successivo o annullato con
   rimborso.
5. **Bot.** Bot di rifornimento con parametri modificabili e simulatore di clienti.
   *Fatto quando* il bot rifornisce da solo e si ferma e riparte dalla sala di
   controllo.
6. **Tempo reale e AsyncAPI.** Hub con `PubSub`, tre stream SSE, presenze, metriche Effect
   (`Metric`), tracer che pubblica le span, `/asyncapi.json`.
   *Fatto quando* `curl -N` su ciascuno stream mostra eventi, presenze e metriche, e il
   documento AsyncAPI passa la validazione.
7. **UI partecipante.** Mobile first: conto e movimenti, ricarica, bonifico, catalogo,
   ordini con stato in diretta, backorder e annullamento, chiusura del conto con la
   conferma di cosa verrà cancellato.
   *Fatto quando* tutto il flusso funziona da un telefono senza ricaricare la pagina.
8. **Sala di controllo.** Presenze, flusso eventi con causazione, saldi e stock, code,
   saghe, grafici delle metriche, trace, prima e dopo dei dati personali, bottoni di
   R17. Pagine `/docs` e AsyncAPI raggiungibili dal menu.
   *Fatto quando* il simulatore muove tutti i pannelli e ogni bottone ha un effetto
   visibile.
9. **Prova generale.** Prova di carico con il simulatore a 50 clienti su Render free;
   limiti di richiesta; script di reset; runbook del giorno (sveglia Supabase, scalda
   Render, piano B col tunnel); pagina nella documentazione.
   *Fatto quando* la prova a 50 clienti regge 10 minuti senza errori e l'invariante dei
   totali resta vero.

Le fasi 3 e 4 possono procedere insieme dopo la 2; la 7 e la 8 dopo la 6.

## 6. Messa online

**Supabase** (solo Postgres)
- Crea il progetto. La stringa di connessione viene da Connect → Session pooler.
- Le tabelle le crea il server al primo avvio, con le migrazioni del framework.

**Render** (Web Service, Docker, branch `main`, piano Free)

| Variabile | Contenuto |
|---|---|
| `POSTGRES_URL` | stringa del session pooler |
| `AUTH_SECRET` | generato da Render |
| `ADMIN_CODE` | il codice del presentatore |
| `PERSONAL_DATA_MASTER_KEY` | 32 byte in base64, da conservare |
| `CORS_ORIGIN` | il dominio Vercel |

**Vercel** (Hobby): root del repo, build `vite build examples/workshop/ui`, output
`examples/workshop/ui/dist`. Una sola variabile: `VITE_API_URL`.

**Il giorno prima.** Apri Supabase, così non va in pausa. Fai il reset della demo e una
prova col simulatore. Tieni pronto sul portatile il container e `cloudflared`.

## 7. Rischi

| Rischio | Contromisura |
|---|---|
| Render free: 512 MB, CPU piccola, si spegne dopo 15 minuti | Prova di carico in fase 9; sveglia prima di iniziare; gli stream aperti lo tengono acceso; piano B col tunnel |
| Riavvio del server a metà workshop | Tutto si ricostruisce dal log; i comandi delle saghe sono idempotenti; test dedicato in fase 4 |
| Supabase in pausa dopo 7 giorni | Riattivarlo il giorno prima |
| Abusi in una demo aperta | Tetti alle ricariche, limiti di richiesta, reset dalla sala di controllo |
| Il Wi-Fi della sala | Nulla da fare per il server remoto; la UI regge le riconnessioni SSE |
| Un partecipante svuota il browser o cambia telefono | Perde il conto: il token sta solo lì. Entra di nuovo con un altro nickname |
| Nuove dipendenze (componente AsyncAPI) | Rispettano `minimumReleaseAge`; nessuna nel pacchetto pubblicato |

## 8. Ancora da decidere

- Quanti partecipanti ti aspetti? Il piano assume fino a 50.
- La lingua della UI: italiano, inglese o entrambe?
- Il tema del catalogo, per esempio gadget, caffè o libri, e quanti prodotti.
- La sala di controllo deve essere visibile anche ai partecipanti, in sola lettura?
- La data del workshop, per pianificare la prova generale.
