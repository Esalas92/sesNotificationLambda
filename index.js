exports.handler = async (event) => {
  console.log('Lambda invocado: ', JSON.stringify(event, null, 2));
  
  const { aws } = require('aws-sdk');
  const { Client } = require('pg');
  const client = new Client();
  
  console.log('Esperando conexion al servidor');
  await client.connect();
  console.log('Conectado...');
  
  let validSNS = false;
  let SESMessage = JSON.parse(event.Records[0].Sns.Message); 
  let SESnotification = {
    eventType: SESMessage.eventType,
    eventDate: null,
    eventTime: null,
    message: null,
    messageId: SESMessage.mail.messageId,
    destination: SESMessage.mail.destination.toString(),
    sender: SESMessage.mail.source.toString(),
    subject: SESMessage.mail.commonHeaders.subject,
    sentDate: SESMessage.mail.timestamp,
    sentTime: SESMessage.mail.timestamp.split('T').pop().split('.')[0],
    feedbackId: null,
    reportingMTA: null,
    eventSubType : null,
    smtpResponse: null,
    userAgent: null,
    ipAddress: null,
    link: null,
    templateName: null,
    errorMessage: null
  };
  
  switch(SESnotification.eventType){
    case 'Bounce':
      SESnotification.reportingMTA = SESMessage.bounce.reportingMTA;
      SESnotification.eventSubType = SESMessage.bounce.bounceSubType;
      SESnotification.feedbackId = SESMessage.bounce.feedbackId;
      SESnotification.eventDate = SESMessage.bounce.timestamp;
      SESnotification.eventTime = SESMessage.bounce.timestamp.split('T').pop().split('.')[0];
      validSNS = true;
      break;
    case 'Complaint':
      SESnotification.userAgent = SESMessage.complaint.userAgent;
      SESnotification.eventSubType = SESMessage.complaint.complaintFeedbackType;
      SESnotification.feedbackId = SESMessage.complaint.feedbackId;
      SESnotification.eventDate = SESMessage.complaint.timestamp;
      SESnotification.eventTime = SESMessage.complaint.timestamp.split('T').pop().split('.')[0];
      validSNS = true;
      break;
    case 'Delivery':
      SESnotification.eventDate = SESMessage.delivery.timestamp;
      SESnotification.eventTime = SESMessage.delivery.timestamp.split('T').pop().split('.')[0];
      SESnotification.smtpResponse = SESMessage.delivery.smtpResponse;
      SESnotification.reportingMTA = SESMessage.delivery.reportingMTA;
      validSNS = true;
      break;
    case 'Reject':
      SESnotification.smtpResponse = SESMessage.reject.reason;
      validSNS = true;
      break;
    case 'Open':
      SESnotification.userAgent = SESMessage.open.userAgent;
      SESnotification.eventDate = SESMessage.open.timestamp;
      SESnotification.eventTime = SESMessage.open.timestamp.split('T').pop().split('.')[0];
      SESnotification.ipAddress = SESMessage.open.ipAddress;
      validSNS = true;
      break;
    case 'Click':
      SESnotification.userAgent = SESMessage.click.userAgent;
      SESnotification.eventDate = SESMessage.click.timestamp;
      SESnotification.eventTime = SESMessage.click.timestamp.split('T').pop().split('.')[0];
      SESnotification.ipAddress = SESMessage.click.ipAddress;
      SESnotification.link = SESMessage.click.link;
      validSNS = true;
      break;
    case 'Rendering Failure':
      SESnotification.errorMessage = SESMessage.failure.errorMessage;
      SESnotification.templateName = SESMessage.failure.templateName;
      validSNS = true;
      break;
  }

  let res = SESMessage;
  if (validSNS) {
      res = await client.query(`
        INSERT INTO entrega2.entrega2.ses_notification (tipo_evento, fec_evento, hor_evento, mensaje, mensaje_id, destinatario, emisor, asunto, fec_envio, hor_envio, feedback_id, 
  	      mta, detalle_evento, respuesta_smtp, dispositivo, ip_destinatario, link, template, mensaje_error)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19);
      `, [SESnotification.eventType, 
          SESnotification.eventDate,
          SESnotification.eventTime,
          SESnotification.message,
          SESnotification.messageId,
          SESnotification.destination,
          SESnotification.sender,
          SESnotification.subject,
          SESnotification.sentDate,
          SESnotification.sentTime,
          SESnotification.feedbackId,
          SESnotification.reportingMTA,
          SESnotification.eventSubType,
          SESnotification.smtpResponse,
          SESnotification.userAgent,
          SESnotification.ipAddress,
          SESnotification.link,
          SESnotification.templateName,
          SESnotification.errorMessage]);
    
    await client.end();
  }
  const response = {
      statusCode: 200,
      result: JSON.stringify(res)
  };
  return response;
};