/**
 * NEQU Remote Desktop — Signaling Server
 * Lekki serwer WebSocket do wymiany SDP/ICE kandydatów między hostem a viewerem.
 * NIE przesyła wideo/audio — tylko metadane połączenia WebRTC.
 *
 * Uruchomienie: node server.js
 * Port: 8765 (lub PORT env)
 */

const WebSocket = require('ws')
const crypto    = require('crypto')

const PORT     = process.env.PORT || 8765
const wss      = new WebSocket.Server({ port: PORT, host: '0.0.0.0' })

// Mapa aktywnych sesji: sessionCode -> { host, viewer, permissions, createdAt, hostId }
const sessions = new Map()

// Mapa połączeń: ws -> { role, sessionCode, peerId }
const peers    = new Map()

const SESSION_TTL_MS = 10 * 60 * 1000 // 10 minut

function log(...args) {
  console.log(`[${new Date().toISOString()}]`, ...args)
}

/** Generuj 6-cyfrowy kod sesji (format: NNN NNN) */
function genCode() {
  let code
  do {
    code = String(Math.floor(100000 + Math.random() * 900000))
  } while (sessions.has(code))
  return code
}

/** Wyślij JSON do klienta */
function send(ws, obj) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    try { ws.send(JSON.stringify(obj)) } catch {}
  }
}

/** Wyczyść przeterminowane sesje */
function cleanupSessions() {
  const now = Date.now()
  for (const [code, sess] of sessions) {
    if (now - sess.createdAt > SESSION_TTL_MS) {
      log(`[CLEANUP] Sesja ${code} wygasła`)
      if (sess.host) send(sess.host, { type: 'session-expired', code })
      if (sess.viewer) send(sess.viewer, { type: 'session-expired', code })
      sessions.delete(code)
    }
  }
}

setInterval(cleanupSessions, 30_000)

wss.on('connection', (ws, req) => {
  const ip = req.socket.remoteAddress
  log(`[CONNECT] ${ip}`)
  peers.set(ws, { role: null, sessionCode: null, peerId: crypto.randomBytes(8).toString('hex') })

  ws.on('message', (raw) => {
    let msg
    try { msg = JSON.parse(raw.toString()) } catch { return }

    const peer = peers.get(ws)

    switch (msg.type) {

      // ── HOST tworzy nową sesję ────────────────────────────────────────
      case 'create-session': {
        const code = genCode()
        const session = {
          host:        ws,
          viewer:      null,
          permissions: msg.permissions || { view: true, control: false, clipboard: false, audio: false },
          createdAt:   Date.now(),
          hostId:      peer.peerId,
          hostName:    msg.hostName || 'Host',
        }
        sessions.set(code, session)
        peer.role        = 'host'
        peer.sessionCode = code

        send(ws, { type: 'session-created', code, expiresIn: SESSION_TTL_MS })
        log(`[SESSION] Utworzono ${code} przez ${ip}`)
        break
      }

      // ── VIEWER dołącza do sesji ───────────────────────────────────────
      case 'join-session': {
        const { code, viewerName } = msg
        const session = sessions.get(code)

        if (!session) {
          send(ws, { type: 'error', reason: 'invalid-code', message: 'Nieprawidłowy kod sesji lub sesja wygasła.' })
          return
        }
        if (session.viewer) {
          send(ws, { type: 'error', reason: 'session-full', message: 'Sesja jest już zajęta.' })
          return
        }

        session.viewer   = ws
        peer.role        = 'viewer'
        peer.sessionCode = code

        // Powiadom viewera o sukcesie + uprawnieniach
        send(ws, {
          type:        'session-joined',
          code,
          permissions: session.permissions,
          hostName:    session.hostName,
        })

        // Powiadom hosta że viewer chce się połączyć (host musi zaakceptować)
        send(session.host, {
          type:       'viewer-waiting',
          viewerName: viewerName || 'Widz',
          viewerId:   peer.peerId,
        })

        log(`[JOIN] ${viewerName || 'Widz'} dołącza do sesji ${code}`)
        break
      }

      // ── HOST akceptuje lub odrzuca viewera ───────────────────────────
      case 'accept-viewer': {
        const session = sessions.get(peer.sessionCode)
        if (!session || peer.role !== 'host') return

        if (msg.accepted) {
          send(session.viewer, { type: 'accepted', permissions: session.permissions })
          log(`[ACCEPT] Host zaakceptował viewera w sesji ${peer.sessionCode}`)
        } else {
          send(session.viewer, { type: 'rejected', reason: msg.reason || 'Host odrzucił połączenie.' })
          session.viewer = null
          log(`[REJECT] Host odrzucił viewera w sesji ${peer.sessionCode}`)
        }
        break
      }

      // ── WebRTC: SDP offer (host → viewer) ────────────────────────────
      case 'offer': {
        const session = sessions.get(peer.sessionCode)
        if (!session || peer.role !== 'host' || !session.viewer) return
        send(session.viewer, { type: 'offer', sdp: msg.sdp })
        break
      }

      // ── WebRTC: SDP answer (viewer → host) ───────────────────────────
      case 'answer': {
        const session = sessions.get(peer.sessionCode)
        if (!session || peer.role !== 'viewer' || !session.host) return
        send(session.host, { type: 'answer', sdp: msg.sdp })
        break
      }

      // ── WebRTC: ICE candidate (obie strony) ──────────────────────────
      case 'ice-candidate': {
        const session = sessions.get(peer.sessionCode)
        if (!session) return
        const target = peer.role === 'host' ? session.viewer : session.host
        send(target, { type: 'ice-candidate', candidate: msg.candidate })
        break
      }

      // ── HOST aktualizuje uprawnienia w trakcie sesji ─────────────────
      case 'update-permissions': {
        const session = sessions.get(peer.sessionCode)
        if (!session || peer.role !== 'host') return
        session.permissions = { ...session.permissions, ...msg.permissions }
        send(session.viewer, { type: 'permissions-updated', permissions: session.permissions })
        log(`[PERMS] Sesja ${peer.sessionCode} uprawnienia zaktualizowane`)
        break
      }

      // ── Zakończenie sesji ────────────────────────────────────────────
      case 'end-session': {
        const code    = peer.sessionCode
        const session = sessions.get(code)
        if (!session) return

        const other = peer.role === 'host' ? session.viewer : session.host
        send(other, { type: 'session-ended', reason: peer.role === 'host' ? 'host-ended' : 'viewer-left' })
        sessions.delete(code)
        log(`[END] Sesja ${code} zakończona przez ${peer.role}`)
        break
      }

      // ── Ping/Pong keep-alive ─────────────────────────────────────────
      case 'ping':
        send(ws, { type: 'pong' })
        break
    }
  })

  ws.on('close', () => {
    const peer    = peers.get(ws)
    const code    = peer?.sessionCode
    const session = code ? sessions.get(code) : null

    if (session) {
      const other = peer.role === 'host' ? session.viewer : session.host
      send(other, { type: 'session-ended', reason: peer.role === 'host' ? 'host-disconnected' : 'viewer-disconnected' })
      if (peer.role === 'host') {
        sessions.delete(code)
        log(`[CLOSE] Host rozłączył się, sesja ${code} usunięta`)
      } else {
        session.viewer = null
        log(`[CLOSE] Viewer rozłączył się z sesji ${code}`)
      }
    }

    peers.delete(ws)
    log(`[DISCONNECT] ${ip}`)
  })

  ws.on('error', () => {})
})

log(`NEQU Signaling Server nasłuchuje na porcie ${PORT}`)
