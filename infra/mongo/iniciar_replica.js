// Inicializa el replica set rs0 y crea los usuarios. Idempotente.
// Corre con mongosh dentro del espacio de red de mongo-1 (network_mode: service:mongo-1): la conexión
// por 127.0.0.1 permite la "localhost exception", que deja crear el primer usuario cuando aún no hay ninguno.
// Variables: MONGO_MIEMBROS (host:puerto separados por coma; el primero es el preferido como primario),
// MONGO_ADMIN_USUARIO, MONGO_ADMIN_CLAVE, MONGO_APP_USUARIO, MONGO_APP_CLAVE.

const miembros = process.env.MONGO_MIEMBROS.split(",").map((h) => h.trim()).filter(Boolean);
const admin = { user: process.env.MONGO_ADMIN_USUARIO, pwd: process.env.MONGO_ADMIN_CLAVE };
const app = { user: process.env.MONGO_APP_USUARIO, pwd: process.env.MONGO_APP_CLAVE };
const adminDb = db.getSiblingDB("admin");

function log(msg) { print(`[mongo-init] ${msg}`); }

function autenticar() {
  try { return adminDb.auth(admin.user, admin.pwd).ok === 1; } catch (e) { return false; }
}

// 1. Replica set
let iniciado = true;
try {
  rs.status();
} catch (e) {
  if (e.codeName === "NotYetInitialized") {
    iniciado = false;
  } else if (e.codeName === "Unauthorized") {
    if (!autenticar()) throw new Error("ya hay usuarios y las credenciales de admin no sirven");
  } else {
    throw e;
  }
}
if (!iniciado) {
  rs.initiate({
    _id: "rs0",
    settings: { electionTimeoutMillis: 2000 },  // RNF-04: elección rápida al caer el primario
    members: miembros.map((host, i) => ({ _id: i, host, priority: i === 0 ? 2 : 1 })),
  });
  log(`replica set rs0 iniciado con ${miembros.length} miembro(s)`);
}

// 2. Esperar a que este nodo sea primario (los usuarios se crean en el primario)
for (let i = 0; i < 60 && !db.hello().isWritablePrimary; i++) sleep(1000);
if (!db.hello().isWritablePrimary) throw new Error("mongo-1 no llegó a primario en 60 s");

// 3. Usuarios: admin (root) y el usuario de la aplicación (readWrite sobre la base breb)
if (!autenticar()) {
  adminDb.createUser({ user: admin.user, pwd: admin.pwd, roles: [{ role: "root", db: "admin" }] });
  log(`usuario ${admin.user} creado`);
  if (!autenticar()) throw new Error("no pude autenticar con el usuario admin recién creado");
}
if (adminDb.getUser(app.user) === null) {
  adminDb.createUser({ user: app.user, pwd: app.pwd, roles: [{ role: "readWrite", db: "breb" }] });
  log(`usuario ${app.user} creado`);
}

// 4. Comprobar que la configuración coincide con el perfil (dev = 1 miembro, completo = 3)
const conf = rs.conf();
const actuales = conf.members.map((m) => m.host).sort().join(",");
if (actuales !== [...miembros].sort().join(",")) {
  throw new Error(`el replica set tiene ${actuales} y el perfil pide ${miembros.join(",")}. ` +
                  "Al cambiar de perfil hay que borrar los volúmenes: docker compose down -v");
}
log(`listo: rs0 con ${conf.members.length} miembro(s), primario ${db.hello().me}, ` +
    `electionTimeoutMillis=${conf.settings.electionTimeoutMillis}`);
