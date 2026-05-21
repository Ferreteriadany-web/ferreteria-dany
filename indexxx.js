const functions = require("firebase-functions/v2");
const { onSchedule } = require("firebase-functions/v2/scheduler");
const { onCall, HttpsError } = require("firebase-functions/v2/https");
const admin = require("firebase-admin");
const axios = require("axios");
const { defineString } = require("firebase-functions/params");

admin.initializeApp();
const db = admin.firestore();

const DUX_TOKEN = defineString("DUX_TOKEN");
const DUX_BASE  = "https://erp.duxsoftware.com.ar/WSERP/rest/services";
const DELAY_MS  = 6000;  // 6 segundos (margen sobre los 5s de DUX)
const LIMIT     = 50;    // máximo permitido por DUX

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function formatFechaDux(date) {
  const dd   = String(date.getDate()).padStart(2,"0");
  const mm   = String(date.getMonth()+1).padStart(2,"0");
  const yyyy = date.getFullYear();
  const hh   = String(date.getHours()).padStart(2,"0");
  const mn   = String(date.getMinutes()).padStart(2,"0");
  return dd+mm+yyyy+" "+hh+":"+mn;
}

async function guardarBatch(items) {
  for (let i = 0; i < items.length; i += 490) {
    const chunk = items.slice(i, i + 490);
    const batch = db.batch();
    for (const item of chunk) {
      if (!item.id) continue;
      const ref = db.collection("productos_dux").doc(String(item.id));
      batch.set(ref, item.data, { merge: true });
    }
    await batch.commit();
  }
}

// ── LOCK CON FIRESTORE para evitar ejecuciones paralelas ──────────────────
async function adquirirLock() {
  const lockRef = db.collection("config").doc("dux_lock");
  return db.runTransaction(async (tx) => {
    const lockDoc = await tx.get(lockRef);
    const ahora = Date.now();

    if (lockDoc.exists) {
      const data = lockDoc.data();
      const lockTime = data.timestamp ? new Date(data.timestamp).getTime() : 0;
      // Si el lock tiene menos de 10 minutos, está vigente
      if (ahora - lockTime < 600000) {
        throw new Error("Ya hay una sincronizacion en curso. Espera 10 min.");
      }
    }

    tx.set(lockRef, {
      timestamp: new Date().toISOString(),
      pid: Math.random().toString(36).substring(7)
    });
    return true;
  });
}

async function liberarLock() {
  try {
    await db.collection("config").doc("dux_lock").delete();
  } catch (e) {
    console.error("Error liberando lock:", e.message);
  }
}

async function syncProductos(incremental) {
  // ADQUIRIR LOCK — si falla, otra ejecución está corriendo
  try {
    await adquirirLock();
    console.log("Lock adquirido, iniciando sync");
  } catch (lockErr) {
    console.warn("No se pudo adquirir lock:", lockErr.message);
    return { ok: false, mensaje: lockErr.message };
  }

  try {
    const configRef  = db.collection("config").doc("dux_sync");
    const configSnap = await configRef.get();
    const ultimaSync = (incremental && configSnap.exists)
      ? configSnap.data().ultimaSync : null;
    const fechaParam = ultimaSync ? formatFechaDux(new Date(ultimaSync)) : null;

    let offset = 0, total = 0, hayMas = true, buffer = [];
    let errorConsecutivos = 0;

    console.log("Sync incremental:", incremental, "Fecha:", fechaParam);

    while (hayMas) {
      try {
        const params = { offset, limit: LIMIT, habilitado: "SI" };
        if (fechaParam) params.fecha = fechaParam;

        const resp = await axios.get(DUX_BASE + "/items", {
          headers: { Authorization: DUX_TOKEN.value() },
          params,
          timeout: 30000
        });

        errorConsecutivos = 0;
        const items = resp.data;
        if (!items || !Array.isArray(items) || items.length === 0) {
          hayMas = false;
          break;
        }

        for (const item of items) {
          buffer.push({
            id: item.codigoItem,
            data: {
              codigo: item.codigoItem || "",
              descripcion: item.descripcion || "",
              proveedor: item.proveedor || "",
              idProveedor: item.idProveedor || 0,
              precioCosto: item.precioCosto || 0,
              precioVenta: item.precio || 0,
              stock: item.stock || 0,
              rubro: item.rubro || "",
              updatedAt: new Date().toISOString()
            }
          });
        }

        if (buffer.length >= 400) {
          await guardarBatch(buffer);
          total += buffer.length;
          buffer = [];
          await configRef.set({ progreso: total, estado: "sincronizando" }, { merge: true });
          console.log("Progreso: " + total + " productos");
        }

        if (items.length < LIMIT) {
          hayMas = false;
        } else {
          offset += LIMIT;
          await sleep(DELAY_MS);
        }
      } catch (err) {
        errorConsecutivos++;
        const statusCode = err.response ? err.response.status : 0;
        console.error("Error DUX (status " + statusCode + "):", err.message);

        if (statusCode === 429) {
          // Esperar agresivamente y mantener el ritmo
          const waitTime = 60000;
          console.log("Rate limit, esperando " + (waitTime/1000) + "s...");
          await sleep(waitTime);
        } else if (statusCode === 401 || statusCode === 403) {
          await configRef.set({
            estado: "error",
            mensaje: "Token DUX invalido o sin permisos"
          }, { merge: true });
          return;
        } else if (errorConsecutivos > 5) {
          await configRef.set({
            estado: "error",
            mensaje: "Demasiados errores: " + err.message
          }, { merge: true });
          return;
        } else {
          await sleep(DELAY_MS * 2);
        }
      }
    }

    if (buffer.length > 0) {
      await guardarBatch(buffer);
      total += buffer.length;
    }

    await configRef.set({
      ultimaSync: new Date().toISOString(),
      totalProductos: total,
      progreso: total,
      estado: "completado",
      tipoUltimaSync: fechaParam ? "incremental" : "completa"
    }, { merge: true });

    console.log("Sync completa: " + total + " productos");
  } finally {
    // LIBERAR LOCK siempre
    await liberarLock();
    console.log("Lock liberado");
  }
}

exports.sincronizarDux = onSchedule(
  { schedule: "0 2 * * *", timeZone: "America/Argentina/Buenos_Aires", timeoutSeconds: 540, memory: "512MiB" },
  async () => { await syncProductos(true); }
);

exports.sincronizarManual = onCall(
  { timeoutSeconds: 540, memory: "512MiB" },
  async (request) => {
    if (!request.auth) throw new HttpsError("unauthenticated", "No autenticado");

    const adminDoc = await db.collection("config").doc("admins").get();
    const admins = adminDoc.exists ? (adminDoc.data().emails || []) : [];
    if (!admins.includes(request.auth.token.email)) {
      throw new HttpsError("permission-denied", "Solo administradores");
    }

    // Verificar si ya hay un lock activo
    const lockDoc = await db.collection("config").doc("dux_lock").get();
    if (lockDoc.exists) {
      const lockTime = new Date(lockDoc.data().timestamp || 0).getTime();
      if (Date.now() - lockTime < 600000) {
        return { ok: false, mensaje: "Ya hay una sincronizacion en curso" };
      }
    }

    await db.collection("config").doc("dux_sync").set(
      { estado: "sincronizando", inicio: new Date().toISOString(), progreso: 0 },
      { merge: true }
    );

    // Disparar sync en background
    syncProductos(request.data.incremental !== false).catch(err => {
      db.collection("config").doc("dux_sync").set({
        estado: "error",
        mensaje: err.message
      }, { merge: true });
    });

    return { ok: true, mensaje: "Sincronizacion iniciada" };
  }
);

// Función para resetear el lock manualmente si se quedó trabado
exports.resetLock = onCall(
  { timeoutSeconds: 60, memory: "256MiB" },
  async (request) => {
    if (!request.auth) throw new HttpsError("unauthenticated", "No autenticado");
    const adminDoc = await db.collection("config").doc("admins").get();
    const admins = adminDoc.exists ? (adminDoc.data().emails || []) : [];
    if (!admins.includes(request.auth.token.email)) {
      throw new HttpsError("permission-denied", "Solo administradores");
    }
    await db.collection("config").doc("dux_lock").delete();
    await db.collection("config").doc("dux_sync").set(
      { estado: "completado" }, { merge: true }
    );
    return { ok: true, mensaje: "Lock liberado" };
  }
);
