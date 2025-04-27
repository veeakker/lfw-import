import { query, sparqlEscapeUri } from 'mu';

/**
 * Return true if the current user is an admin user and false otherwise.
 */
export async function isAdminUser(req) {
  const sessionId = req.headers["mu-session-id"];

  const resp = (await query(`
  PREFIX session: <http://mu.semte.ch/vocabularies/session/>
  PREFIX foaf: <http://xmlns.com/foaf/0.1/>
  PREFIX veeakker: <http://veeakker.be/vocabularies/shop/>
  ASK {
      ${sparqlEscapeUri(sessionId)} session:account/^foaf:account/veeakker:role veeakker:Administrator.
      }`, { sudo: true }));

  return resp.boolean;
}
