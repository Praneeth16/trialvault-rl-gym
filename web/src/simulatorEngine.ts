/**
 * Deterministic client-side replica of TrialVault's 5-store consent withdrawal engine.
 * Models the exact HIPAA Safe Harbor + FDA 21 CFR Part 11 + FDA 2008 specimen boundary rules.
 */

export type SubjectScenarioId = 'elena' | 'arthur' | 'priya' | 'marcus';

export type SimAction =
  | 'post_withdrawal'
  | 'run_worker'
  | 'advance_clock'
  | 'webhook_post_withdrawal'
  | 'webhook_pre_withdrawal'
  | 'inject_blob_fault'
  | 'recover_blob_fault';

export interface SimSubjectRecord {
  subject_id: string;
  orig_subject_id: string;
  mrn: string | null;
  orig_mrn: string;
  full_name: string | null;
  orig_full_name: string;
  email: string | null;
  phone: string | null;
  ssn: string | null;
  dob: string | null;
  age_at_enrollment: number | null;
  age_bucket: string;
  street_address: string | null;
  city: string | null;
  state: string;
  postal_code: string;
  treatment_arm: string;
  randomization_date: string | null;
  status: 'active' | 'withdrawn';
  regulatory_hold: boolean;
  hold_reason: string | null;
  anon_subject_ref: string | null;
  household_group_id: string | null;
  roleNote: string;
}

export interface SimResponse {
  method: string;
  path: string;
  httpStatus: number;
  tone: 'pass' | 'fail' | 'warn';
  summary: string;
  body: Record<string, unknown>;
}

export interface SimState {
  subject: SimSubjectRecord;
  requestStatus: 'none' | 'pending' | 'in_progress' | 'completed' | 'blocked_regulatory_hold';
  requestId: string | null;
  effectiveDate: string;
  blobFaultActive: boolean;
  clockIso: string;
  clinicalNotes: string;
  labRows: Array<{
    accession_no: string;
    subject_id: string;
    analyte: string;
    value_num: number;
    collected_date: string | null;
    study_day: number;
    raw_hl7_pid: Record<string, string | null>;
  }>;
  ftsIndexed: boolean;
  cacheKeys: string[];
  econsentPresent: boolean;
  labBlobMeta: Record<string, string>;
  dsmbSha256: string;
  dsmbChecksumValid: boolean;
  dsmbSubjectEntry: Record<string, unknown>;
  scheduledDay30Status: 'scheduled' | 'cancelled' | 'executed';
  outboundSpoolEntries: string[];
  dlqEntries: string[];
  auditPiiCount: number;
  auditChainEntries: Array<{ seq: number; action: string; resource_ref: string; entry_hash: string }>;
  safetySummary: {
    total_subjects: number;
    drug_a_subjects: number;
    placebo_subjects: number;
    total_aes: number;
    alt_count: number;
    alt_mean: number;
  };
  lastResponse: SimResponse | null;
  log: Array<{ step: string; detail: string }>;
}

const RESTRICTED_ZIP3 = new Set([
  '036', '059', '063', '102', '203', '556', '692', '790',
  '821', '823', '830', '831', '878', '879', '884', '890', '893',
]);

function shortHash(seed: string): string {
  let h1 = 0xdeadbeef ^ seed.length;
  let h2 = 0x41c6ce57 ^ seed.length;
  for (let i = 0; i < seed.length; i++) {
    const ch = seed.charCodeAt(i);
    h1 = Math.imul(h1 ^ ch, 2654435761);
    h2 = Math.imul(h2 ^ ch, 1597334677);
  }
  const a = (h1 >>> 0).toString(16).padStart(8, '0');
  const b = (h2 >>> 0).toString(16).padStart(8, '0');
  return `${a}${b}`;
}

export const SUBJECT_SCENARIOS: Array<{
  id: SubjectScenarioId;
  name: string;
  subjectId: string;
  subtitle: string;
}> = [
  {
    id: 'elena',
    name: 'Elena Rostova',
    subjectId: 'SUBJ-BOS-1001',
    subtitle: 'Age 52, ZIP 02115, co-enrolled household',
  },
  {
    id: 'arthur',
    name: 'Arthur Pendelton',
    subjectId: 'SUBJ-BOS-1003',
    subtitle: 'Age 92 (>89 rule), restricted ZIP3 03603',
  },
  {
    id: 'priya',
    name: 'Priya Nair',
    subjectId: 'SUBJ-BOS-HOLD',
    subtitle: 'Active FDA Form 483 regulatory hold',
  },
  {
    id: 'marcus',
    name: 'Marcus Rostov',
    subjectId: 'SUBJ-BOS-1002',
    subtitle: 'Co-enrolled household survivor control',
  },
];

export function createInitialSimState(id: SubjectScenarioId): SimState {
  const templates: Record<SubjectScenarioId, SimSubjectRecord> = {
    elena: {
      subject_id: 'SUBJ-BOS-1001',
      orig_subject_id: 'SUBJ-BOS-1001',
      mrn: 'MRN-8841201',
      orig_mrn: 'MRN-8841201',
      full_name: 'Elena Rostova',
      orig_full_name: 'Elena Rostova',
      email: 'elena.rostova@example.org',
      phone: '+1-617-555-0142',
      ssn: '123-45-6789',
      dob: '1974-08-19',
      age_at_enrollment: 52,
      age_bucket: '52',
      street_address: '742 Beacon St Apt 4B',
      city: 'Boston',
      state: 'MA',
      postal_code: '02115',
      treatment_arm: 'DRUG_A_100MG',
      randomization_date: '2026-02-10',
      status: 'active',
      regulatory_hold: false,
      hold_reason: null,
      anon_subject_ref: null,
      household_group_id: 'HH-BEACON-742',
      roleNote: 'Standard adult subject sharing 742 Beacon St with active survivor Marcus Rostov.',
    },
    arthur: {
      subject_id: 'SUBJ-BOS-1003',
      orig_subject_id: 'SUBJ-BOS-1003',
      mrn: 'MRN-8841203',
      orig_mrn: 'MRN-8841203',
      full_name: 'Arthur Pendelton',
      orig_full_name: 'Arthur Pendelton',
      email: 'arthur.pendelton@example.org',
      phone: '+1-603-555-0188',
      ssn: '345-67-8901',
      dob: '1933-11-02',
      age_at_enrollment: 92,
      age_bucket: '90+',
      street_address: '19 Pine Hollow Rd',
      city: 'Charlestown',
      state: 'NH',
      postal_code: '03603',
      treatment_arm: 'DRUG_A_100MG',
      randomization_date: '2026-02-18',
      status: 'active',
      regulatory_hold: false,
      hold_reason: null,
      anon_subject_ref: null,
      household_group_id: null,
      roleNote: 'Tests Safe Harbor age > 89 (null age and DOB, bucket 90+) and restricted ZIP3 036 -> 000.',
    },
    priya: {
      subject_id: 'SUBJ-BOS-HOLD',
      orig_subject_id: 'SUBJ-BOS-HOLD',
      mrn: 'MRN-8841299',
      orig_mrn: 'MRN-8841299',
      full_name: 'Priya Nair',
      orig_full_name: 'Priya Nair',
      email: 'priya.nair@example.org',
      phone: '+1-617-555-0177',
      ssn: '456-78-9012',
      dob: '1980-02-12',
      age_at_enrollment: 46,
      age_bucket: '46',
      street_address: '88 Newbury St',
      city: 'Boston',
      state: 'MA',
      postal_code: '02116',
      treatment_arm: 'DRUG_A_100MG',
      randomization_date: '2026-02-20',
      status: 'active',
      regulatory_hold: true,
      hold_reason: 'FDA Form 483 clinical inspection hold',
      anon_subject_ref: null,
      household_group_id: null,
      roleNote: 'Withdrawal must return HTTP 409 blocked_regulatory_hold with zero mutations to any store.',
    },
    marcus: {
      subject_id: 'SUBJ-BOS-1002',
      orig_subject_id: 'SUBJ-BOS-1002',
      mrn: 'MRN-8841202',
      orig_mrn: 'MRN-8841202',
      full_name: 'Marcus Rostov',
      orig_full_name: 'Marcus Rostov',
      email: 'marcus.rostov@example.org',
      phone: '+1-617-555-0143',
      ssn: '234-56-7890',
      dob: '1971-04-03',
      age_at_enrollment: 55,
      age_bucket: '55',
      street_address: '742 Beacon St Apt 4B',
      city: 'Boston',
      state: 'MA',
      postal_code: '02115',
      treatment_arm: 'PLACEBO',
      randomization_date: '2026-02-12',
      status: 'active',
      regulatory_hold: false,
      hold_reason: null,
      anon_subject_ref: null,
      household_group_id: 'HH-BEACON-742',
      roleNote: 'Active control sharing street address and ZIP 02115; remains intact when Elena withdraws.',
    },
  };

  const subj = templates[id];
  const acc = `ACC-${subj.subject_id.slice(-4)}`;
  const dsmbEntry = {
    subject_id: subj.subject_id,
    mrn: subj.mrn,
    full_name: subj.full_name,
    dob: subj.dob,
    postal_code: subj.postal_code,
    treatment_arm: subj.treatment_arm,
    status: 'active',
    adverse_events_count: 1,
  };
  const h1 = shortHash(`enroll:${subj.subject_id}`);
  const h2 = shortHash(`${h1}:lab:${acc}`);

  return {
    subject: { ...subj },
    requestStatus: 'none',
    requestId: null,
    effectiveDate: '2026-03-10',
    blobFaultActive: false,
    clockIso: '2026-03-10T12:00:00Z',
    clinicalNotes: `Evaluated ${subj.full_name} (${subj.mrn}, phone ${subj.phone}) on 2026-02-24. Stable hemodynamics.`,
    labRows: [
      {
        accession_no: acc,
        subject_id: subj.subject_id,
        analyte: 'ALT',
        value_num: id === 'arthur' ? 36.5 : 44.0,
        collected_date: '2026-02-24',
        study_day: 14,
        raw_hl7_pid: {
          subject_id: subj.subject_id,
          mrn: subj.mrn,
          patient_name: subj.full_name,
          dob: subj.dob,
        },
      },
    ],
    ftsIndexed: true,
    cacheKeys: [
      `site:SITE-BOS-01:subject:${subj.subject_id}:profile`,
      `site:SITE-BOS-01:subject:${subj.subject_id}:timeline`,
      `site:SITE-BOS-01:mrn:${subj.mrn}`,
      'trial:TRIAL-ONC-301:safety_summary',
    ],
    econsentPresent: true,
    labBlobMeta: {
      doc_type: 'central_lab',
      accession_no: acc,
      subject_id: subj.subject_id,
      mrn: subj.mrn || '',
      patient_name: subj.full_name || '',
      collected_date: '2026-02-24',
    },
    dsmbSha256: shortHash(JSON.stringify(dsmbEntry)),
    dsmbChecksumValid: true,
    dsmbSubjectEntry: dsmbEntry,
    scheduledDay30Status: 'scheduled',
    outboundSpoolEntries: [
      `{"subject_id":"${subj.subject_id}","recipient":"${subj.email}","template":"daily_diary_reminder"}`,
      '{"subject_id":"SUBJ-BOS-1002","recipient":"marcus.rostov@example.org","template":"daily_diary_reminder"}',
    ],
    dlqEntries: [
      `{"dlq_id":"DLQ-1","subject_id":"${subj.subject_id}","error":"smtp_timeout"}`,
      '{"dlq_id":"DLQ-2","subject_id":"SUBJ-BOS-1002","error":"smtp_timeout"}',
    ],
    auditPiiCount: 3,
    auditChainEntries: [
      { seq: 1, action: 'SUBJECT_ENROLLED', resource_ref: `subj:${shortHash(subj.subject_id).slice(0, 8)}`, entry_hash: h1 },
      { seq: 2, action: 'LAB_RESULT_INGESTED', resource_ref: `lab:${shortHash(acc).slice(0, 8)}`, entry_hash: h2 },
    ],
    safetySummary: {
      total_subjects: 4,
      drug_a_subjects: 3,
      placebo_subjects: 1,
      total_aes: 3,
      alt_count: 4,
      alt_mean: 34.88,
    },
    lastResponse: null,
    log: [{ step: 'Seeded', detail: `Initialized 5 stores for ${subj.subject_id} (${subj.orig_full_name})` }],
  };
}

export function applySimAction(state: SimState, action: SimAction): SimState {
  const s: SimState = JSON.parse(JSON.stringify(state));
  const origId = s.subject.orig_subject_id;
  const anonRef = s.subject.anon_subject_ref || `ANON-${shortHash(origId).slice(0, 8).toUpperCase()}`;

  if (action === 'post_withdrawal') {
    if (s.subject.regulatory_hold) {
      s.requestStatus = 'blocked_regulatory_hold';
      s.lastResponse = {
        method: 'POST',
        path: `/v1/subjects/${origId}/withdrawal-requests`,
        httpStatus: 409,
        tone: 'warn',
        summary: 'Rejected under FDA regulatory hold with zero store mutations',
        body: {
          error: 'blocked_regulatory_hold',
          status: 'blocked_regulatory_hold',
          detail: s.subject.hold_reason,
        },
      };
      s.log.unshift({ step: 'POST 409', detail: 'Regulatory hold blocked withdrawal; all 5 stores untouched' });
      return s;
    }

    if (s.requestStatus === 'none') {
      s.requestId = `wd-${shortHash(origId).slice(0, 10)}`;
      s.subject.anon_subject_ref = anonRef;
      s.requestStatus = 'pending';
      s.lastResponse = {
        method: 'POST',
        path: `/v1/subjects/${origId}/withdrawal-requests`,
        httpStatus: 202,
        tone: 'pass',
        summary: 'Accepted asynchronously as pending; SHA-256 suppression hashes registered',
        body: {
          request_id: s.requestId,
          site_id: 'SITE-BOS-01',
          status: 'pending',
          anon_subject_ref: anonRef,
          effective_date: s.effectiveDate,
          completed_at: null,
        },
      };
      s.log.unshift({ step: 'POST 202', detail: `Queued ${s.requestId} (status=pending, anon=${anonRef})` });
    } else {
      s.lastResponse = {
        method: 'POST',
        path: `/v1/subjects/${origId}/withdrawal-requests`,
        httpStatus: 202,
        tone: 'pass',
        summary: 'Idempotent replay returned existing request without spawning a competing workflow',
        body: {
          request_id: s.requestId,
          status: s.requestStatus,
          anon_subject_ref: anonRef,
        },
      };
      s.log.unshift({ step: 'POST 202 (replay)', detail: `Returned existing request ${s.requestId}` });
    }
    return s;
  }

  if (action === 'inject_blob_fault') {
    s.blobFaultActive = true;
    s.lastResponse = {
      method: 'FAULT',
      path: `blobs/labs/SITE-BOS-01/${s.labRows[0].accession_no}.json`,
      httpStatus: 500,
      tone: 'warn',
      summary: 'Injected OS-level IsADirectoryError on lab blob file before worker tick',
      body: { fault: 'IsADirectoryError', target: `blobs/labs/SITE-BOS-01/${s.labRows[0].accession_no}.json` },
    };
    s.log.unshift({ step: 'Fault injected', detail: 'Lab blob replaced with directory to force transient OSError' });
    return s;
  }

  if (action === 'recover_blob_fault') {
    s.blobFaultActive = false;
    s.lastResponse = {
      method: 'RECOVER',
      path: `blobs/labs/SITE-BOS-01/${s.labRows[0].accession_no}.json`,
      httpStatus: 200,
      tone: 'pass',
      summary: 'Cleared transient blob fault; ready for idempotent worker retry',
      body: { fault_cleared: true },
    };
    s.log.unshift({ step: 'Fault cleared', detail: 'Restored lab blob file for next worker tick' });
    return s;
  }

  if (action === 'run_worker') {
    if (s.requestStatus === 'none' || s.requestStatus === 'blocked_regulatory_hold') {
      s.lastResponse = {
        method: 'WORKER',
        path: 'python -m trialvault.worker --once',
        httpStatus: 200,
        tone: 'warn',
        summary: 'No pending withdrawal workflow to process',
        body: { processed: 0, failed: 0, suppressed: 0 },
      };
      return s;
    }

    if (s.blobFaultActive && s.requestStatus !== 'completed') {
      s.requestStatus = 'in_progress';
      s.lastResponse = {
        method: 'WORKER',
        path: 'python -m trialvault.worker --once',
        httpStatus: 200,
        tone: 'warn',
        summary: 'Worker caught IsADirectoryError; blocked premature completed status and preserved clinical totals',
        body: {
          request_id: s.requestId,
          status: 'in_progress',
          completed_at: null,
          error: 'IsADirectoryError: lab blob unreadable',
        },
      };
      s.log.unshift({ step: 'Worker retryable fail', detail: 'Blocked premature completion; clinical records consistent' });
      return s;
    }

    if (s.requestStatus !== 'completed') {
      const zip3Raw = s.subject.postal_code.slice(0, 3);
      const newZip = RESTRICTED_ZIP3.has(zip3Raw) ? '000' : zip3Raw;
      const isOver89 = (s.subject.age_at_enrollment ?? 0) > 89;

      s.subject.subject_id = anonRef;
      s.subject.mrn = null;
      s.subject.full_name = null;
      s.subject.email = null;
      s.subject.phone = null;
      s.subject.ssn = null;
      s.subject.street_address = null;
      s.subject.city = null;
      s.subject.postal_code = newZip;
      s.subject.randomization_date = null;
      s.subject.household_group_id = null;
      s.subject.status = 'withdrawn';
      s.subject.age_bucket = isOver89 ? '90+' : String(s.subject.age_at_enrollment);
      s.subject.age_at_enrollment = isOver89 ? null : s.subject.age_at_enrollment;
      s.subject.dob = isOver89 ? null : (s.subject.dob ? s.subject.dob.slice(0, 4) : null);

      s.clinicalNotes = 'Evaluated [REDACTED] ([REDACTED], phone [REDACTED]) on [REDACTED]. Stable hemodynamics.';
      s.labRows = s.labRows.map((lb) => ({
        ...lb,
        subject_id: anonRef,
        collected_date: null,
        raw_hl7_pid: { anon_subject_ref: anonRef },
      }));

      s.ftsIndexed = false;
      s.cacheKeys = ['trial:TRIAL-ONC-301:safety_summary (recomputed)'];
      s.econsentPresent = false;
      s.labBlobMeta = {
        doc_type: 'central_lab',
        accession_no: s.labRows[0].accession_no,
        anon_subject_ref: anonRef,
      };
      s.dsmbSubjectEntry = {
        subject_id: anonRef,
        mrn: null,
        full_name: null,
        dob: s.subject.dob,
        age_bucket: s.subject.age_bucket,
        postal_code: newZip,
        treatment_arm: s.subject.treatment_arm,
        status: 'withdrawn',
        adverse_events_count: 1,
      };
      s.dsmbSha256 = shortHash(JSON.stringify(s.dsmbSubjectEntry));
      s.dsmbChecksumValid = true;
      s.scheduledDay30Status = 'cancelled';
      s.outboundSpoolEntries = s.outboundSpoolEntries.filter((l) => !l.includes(origId));
      s.dlqEntries = s.dlqEntries.filter((l) => !l.includes(origId));
      s.auditPiiCount = 0;

      const prevHash = s.auditChainEntries[s.auditChainEntries.length - 1].entry_hash;
      s.auditChainEntries.push({
        seq: s.auditChainEntries.length + 1,
        action: 'CONSENT_WITHDRAWN',
        resource_ref: anonRef,
        entry_hash: shortHash(`${prevHash}:CONSENT_WITHDRAWN:${anonRef}`),
      });
      s.requestStatus = 'completed';

      s.lastResponse = {
        method: 'WORKER',
        path: 'python -m trialvault.worker --once',
        httpStatus: 200,
        tone: 'pass',
        summary: 'All 5 stores, DSMB manifest, and 21 CFR Part 11 audit chain converged to completed',
        body: {
          request_id: s.requestId,
          status: 'completed',
          anon_subject_ref: anonRef,
          zip3: newZip,
          age_bucket: s.subject.age_bucket,
          audit_chain_valid: true,
        },
      };
      s.log.unshift({ step: 'Converged', detail: `5 stores cleaned, DSMB re-signed, CONSENT_WITHDRAWN chained` });
    }
    return s;
  }

  if (action === 'advance_clock') {
    s.clockIso = '2026-05-01T00:00:00Z';
    if (s.scheduledDay30Status === 'scheduled') {
      s.scheduledDay30Status = 'executed';
      s.outboundSpoolEntries.push(
        `{"subject_id":"${origId}","template":"day30_followup_diary","clock":"2026-05-01"}`
      );
      s.lastResponse = {
        method: 'WORKER',
        path: 'python -m trialvault.worker --once --now 2026-05-01T00:00:00Z',
        httpStatus: 200,
        tone: 'pass',
        summary: 'Active subject day-30 ePRO job fired normally',
        body: { scheduled_ran: 1, suppressed: 0 },
      };
      s.log.unshift({ step: 'Clock 2026-05-01', detail: 'Active subject day-30 reminder dispatched to outbound spool' });
    } else {
      s.lastResponse = {
        method: 'WORKER',
        path: 'python -m trialvault.worker --once --now 2026-05-01T00:00:00Z',
        httpStatus: 200,
        tone: 'pass',
        summary: 'Withdrawn subject day-30 job stayed suppressed while co-enrolled survivor fired normally',
        body: { scheduled_ran: 1, suppressed: 1, survivor_dispatched: 'SUBJ-BOS-1002' },
      };
      s.log.unshift({ step: 'Clock 2026-05-01', detail: 'Delayed job suppressed for withdrawn subject; survivor unaffected' });
    }
    return s;
  }

  if (action === 'webhook_post_withdrawal') {
    const isWithdrawn = s.requestStatus === 'pending' || s.requestStatus === 'in_progress' || s.requestStatus === 'completed';
    if (isWithdrawn) {
      s.lastResponse = {
        method: 'POST',
        path: '/v1/webhooks/central-lab (collected 2026-03-15 > effective 2026-03-10)',
        httpStatus: 202,
        tone: 'pass',
        summary: 'Post-withdrawal specimen suppressed; zero rows or PHI written',
        body: { status: 'suppressed', accession_no: 'ACC-LATE-POST-901' },
      };
      s.log.unshift({ step: 'Webhook 202', detail: 'Post-withdrawal HL7 feed (2026-03-15) suppressed by MRN hash' });
    } else {
      s.labRows.push({
        accession_no: 'ACC-LATE-POST-901',
        subject_id: origId,
        analyte: 'ALT',
        value_num: 39.0,
        collected_date: '2026-03-15',
        study_day: 33,
        raw_hl7_pid: { subject_id: origId, mrn: s.subject.orig_mrn, patient_name: s.subject.orig_full_name },
      });
      s.safetySummary.alt_count += 1;
      s.safetySummary.alt_mean = Number(((s.safetySummary.alt_mean * (s.safetySummary.alt_count - 1) + 39.0) / s.safetySummary.alt_count).toFixed(2));
      s.lastResponse = {
        method: 'POST',
        path: '/v1/webhooks/central-lab (active subject)',
        httpStatus: 201,
        tone: 'pass',
        summary: 'Active subject lab report ingested normally',
        body: { status: 'ingested', accession_no: 'ACC-LATE-POST-901' },
      };
      s.log.unshift({ step: 'Webhook 201', detail: 'Ingested active-subject central-lab report' });
    }
    return s;
  }

  if (action === 'webhook_pre_withdrawal') {
    const isWithdrawn = s.requestStatus === 'completed';
    if (isWithdrawn) {
      s.labRows.push({
        accession_no: 'ACC-LATE-PRE-802',
        subject_id: anonRef,
        analyte: 'ALT',
        value_num: 50.0,
        collected_date: null,
        study_day: 26,
        raw_hl7_pid: { anon_subject_ref: anonRef },
      });
      s.safetySummary.alt_count += 1;
      s.safetySummary.alt_mean = Number(((s.safetySummary.alt_mean * (s.safetySummary.alt_count - 1) + 50.0) / s.safetySummary.alt_count).toFixed(2));
      const prevHash = s.auditChainEntries[s.auditChainEntries.length - 1].entry_hash;
      s.auditChainEntries.push({
        seq: s.auditChainEntries.length + 1,
        action: 'LAB_RESULT_INGESTED_DEIDENTIFIED',
        resource_ref: `lab:${shortHash('ACC-LATE-PRE-802').slice(0, 8)}`,
        entry_hash: shortHash(`${prevHash}:PRE:${anonRef}`),
      });
      s.lastResponse = {
        method: 'POST',
        path: '/v1/webhooks/central-lab (collected 2026-03-08 <= effective 2026-03-10)',
        httpStatus: 201,
        tone: 'pass',
        summary: 'FDA 2008 rule: pre-withdrawal specimen retained de-identified under anon_subject_ref with zero PHI',
        body: { status: 'ingested_deidentified', accession_no: 'ACC-LATE-PRE-802', anon_subject_ref: anonRef },
      };
      s.log.unshift({ step: 'Webhook 201 (FDA)', detail: 'Pre-withdrawal specimen (2026-03-08) ingested de-identified; safety summary updated' });
    } else {
      s.lastResponse = {
        method: 'POST',
        path: '/v1/webhooks/central-lab (collected 2026-03-08)',
        httpStatus: 201,
        tone: 'pass',
        summary: 'Withdraw and run worker first to see pre-withdrawal de-identified routing on a withdrawn subject',
        body: { status: 'ingested', accession_no: 'ACC-LATE-PRE-802' },
      };
    }
    return s;
  }

  return s;
}
