package com.bridgetct.core;

import java.io.UnsupportedEncodingException;
import java.util.Properties;

import javax.mail.Authenticator;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

public class MailTest {
	  public static void main(String[] args)throws MessagingException, UnsupportedEncodingException {
			
			boolean debug = false;
			java.security.Security.addProvider(new com.sun.net.ssl.internal.ssl.Provider());
		  
			String SMTP_HOST_NAME = "gmail-smtp.l.google.com";
			 
			// Properties 설정
			Properties props = new Properties();
			props.put("mail.transport.protocol", "smtp");
			props.put("mail.smtp.starttls.enable","true");
			props.put("mail.smtp.host", SMTP_HOST_NAME);
			props.put("mail.smtp.auth", "true");
		  
			Authenticator auth = new SMTPAuthenticator();
			Session session = Session.getDefaultInstance(props, auth);
		  
			session.setDebug(debug);
		  
			// create a message
			MimeMessage msg = new MimeMessage(session);
		  
			// set the from and to address
			InternetAddress addressFrom = new InternetAddress("molitinter@gmail.com","국제협력정보공유시스템","EUC-KR");
			msg.setFrom(addressFrom);
			String  recipients [] = {"molitinter@gmail.com","leesm1988@naver.com"};
			InternetAddress[] addressTo = new InternetAddress[recipients.length];
			for (int i = 0; i < recipients.length; i++) {
			    addressTo[i] = new InternetAddress(recipients[i]);
			}
			msg.setRecipients(Message.RecipientType.TO, addressTo);
		  
			// Setting the Subject and Content Type
			msg.setSubject("국제협력정보공유시스템에서 안내드립니다.", "UTF-8");
//			message.setContent(mailForm(), "EUC-KR");
				msg.setText(mailForm(), "EUC-KR");
				msg.setHeader("content-Type", "text/html");
			Transport.send(msg);
		  
		}
	    
	  
	    
	    private static String mailForm(){
	    	String mailForm = "";
	    	
	    	mailForm+="<!doctype html>";
	    	mailForm+="<html>";
	    	mailForm+="<head>";
	    	mailForm+="<meta charset='utf-8'>";
	    	mailForm+="<title>국제협력정보 공유시스템</title>";
	    	mailForm+="</head>";
	    	mailForm+="<body>";
	    	mailForm+="<table style='background:#ffffff url('https://global.molit.go.kr/img/bg_tile.jpg')' width='100%' align='center' cellpadding='0' cellspacing='0' border='0'>";
	    	mailForm+="	<tbody>";
	    	mailForm+="		<tr>";
	    	mailForm+="			<td align='center'>";
	    	mailForm+="				<table width='100%' cellpadding='0' cellspacing='0' border='0'>";
	    	mailForm+="					<tbody>";
	    	mailForm+="						<tr>";
	    	mailForm+="							<td height='20'></td>";
	    	mailForm+="						</tr>";
	    	mailForm+="						<tr>";
	    	mailForm+="							<td align='center'><table width='700' cellpadding='0' cellspacing='0' border='0'>";
	    	mailForm+="									<tbody>";
	    	mailForm+="											<td><a href='https://global.molit.go.kr/' title='국제협력정보공유시스템 웹사이트 새창열림' target='_blank'></a></td>";
	    	mailForm+="										</tr>";
	    	mailForm+="									</tbody>";
	    	mailForm+="								</table></td>";
	    	mailForm+="						</tr>";
	    	mailForm+="						<tr>";
	    	mailForm+="							<td height='10'></td>";
	    	mailForm+="						</tr>";
	    	mailForm+="					</tbody>";
	    	mailForm+="				</table>";
	    	mailForm+="				<table width='700' border='0' cellpadding='0' cellspacing='0' align='center'>";
	    	mailForm+="					<tbody>";
	    	mailForm+="						<tr valign='top'>";
	    	mailForm+="							<td>";
	    	mailForm+="								<table width='700' border='0' cellpadding='0' cellspacing='0' align='center' bgcolor='#ffffff' style='background:#ffffff;border:1px solid #d2d2d2;border-radius:20px;'>";
	    	mailForm+="									<tbody>";
	    	mailForm+="										<tr>";
	    	mailForm+="											<td><img src='https://global.molit.go.kr/img/top_guide.png' width='700' height='270' alt='국제협력정보공유시스템에서 안내드립니다.'></td>";
	    	mailForm+="										</tr>";
	    	mailForm+="										<tr>";
	    	mailForm+="											<td height='50'></td>";
	    	mailForm+="										</tr>";
	    	mailForm+="									</tbody>";
	    	mailForm+="								</table>";
	    	mailForm+="								<table width='700' border='0' cellpadding='0' cellspacing='0' align='center' bgcolor='#ffffff' style='background:#ffffff;border:1px solid #d2d2d2;border-radius:20px;margin-top:-1px;'>";
	    	mailForm+="									<tbody>";
	    	mailForm+="										<tr>";
	    	mailForm+="											<td height='10'>&nbsp;</td>";
	    	mailForm+="										</tr>";
	    	mailForm+="										<tr>";
	    	mailForm+="											<td height='10'>&nbsp;</td>";
	    	mailForm+="										</tr>";
	    	mailForm+="										<tr>";
	    	mailForm+="											<td align='center'>";
	    	mailForm+="												<table width='700' border='0' cellpadding='0' cellspacing='0'>";
	    	mailForm+="													<tr>";
	    	mailForm+="														<td width='50'>&nbsp;</td>";
	    	mailForm+="														<td>";
	    	mailForm+="															<p style='font-size:16px;line-height:1.5em;'>";
	    	mailForm+="															안녕하세요. 국제협력정보공유시스템 담당자입니다."; 
	    	mailForm+="															항상 저희 시스템에 더 나은 정보를 제공하기 위해 애써주심에 진심으로 감사드립니다.";
	    	mailForm+="															메일을 드린 이유는 다름이 아니라 3개월동안 담당하시는 정보가 갱신되지 않았기 때문입니다.";
	    	mailForm+="															하단의 버튼을 클릭하시어 관련 정보를 갱신해 주시기 바랍니다.";
	    	mailForm+="															문의사항이 있으시면 아래 연락처로 연락주시기 바랍니다.";
	    	mailForm+="															감사합니다.";
	    	mailForm+="															</p>";
	    	mailForm+="														</td>";
	    	mailForm+="														<td width='50'>&nbsp;</td>";
	    	mailForm+="													</tr>";
	    	mailForm+="												</table>";
	    	mailForm+="											</td>";
	    	mailForm+="										</tr>";
	    	mailForm+="										<tr>";
	    	mailForm+="											<td height='30'>&nbsp;</td>";
	    	mailForm+="										</tr>";
	    	mailForm+="										<tr>";
	    	mailForm+="											<td align='center'><a href='https://global.molit.go.kr/'><img src='https://global.molit.go.kr/img/btn_go.png' width='312' height='55' alt='정보입력하러가기'></a></td>";
	    	mailForm+="										</tr>";
	    	mailForm+="										<tr>";
	    	mailForm+="											<td height='50'>&nbsp;</td>";
	    	mailForm+="										</tr>";
	    	mailForm+="									</tbody>";
	    	mailForm+="								</table>";
	    	mailForm+="								<table width='700' border='0' cellpadding='0' cellspacing='0' align='center' bgcolor='#f1f1f1' style='background:#f1f1f1;border:1px solid #d2d2d2;border-radius:20px;margin-top:-1px;'>";
	    	mailForm+="									<tbody>";
	    	mailForm+="										<tr>";
	    	mailForm+="											<td height='30'>&nbsp;</td>";
	    	mailForm+="										</tr>";
	    	mailForm+="										<tr>";
	    	mailForm+="											<td>";
	    	mailForm+="												<table width='700' border='0' cellpadding='0' cellspacing='0'>";
	    	mailForm+="													<tr>";
	    	mailForm+="														<td width='50'>&nbsp;</td>";
	    	mailForm+="														<td>";
	    	mailForm+="															<p style='font-size:11px;line-height:1.4em;color:#666;'>";
	    	mailForm+="																본 메일은 &lt;정보통신망 이용 촉진 및 정보보호 등에 관한 법률 및 시행규칙&gt;을 준수합니다. <br>";
	    	mailForm+="																본 메일은 발송전용 메일이오니 회신을 통한 문의는 처리 되지 않습니다.<br>";
	    	mailForm+="																문의사항이 있으시면 아래 연락처를 이용해 주시기 바랍니다.<br>";
	    	mailForm+="															</p>";
	    	mailForm+="														</td>";
	    	mailForm+="														<td width='50'>&nbsp;</td>";
	    	mailForm+="													</tr>";
	    	mailForm+="												</table>";
	    	mailForm+="											</td>";
	    	mailForm+="										</tr>";
	    	mailForm+="										<tr>";
	    	mailForm+="											<td height='10'>&nbsp;</td>";
	    	mailForm+="										</tr>";
	    	mailForm+="										<tr>";
	    	mailForm+="																					<td>";
	    	mailForm+="												<table width='700' border='0' cellpadding='0' cellspacing='0'>";
	    	mailForm+="													<tr>";
	    	mailForm+="										<td width='50'>&nbsp;</td>";
	    	mailForm+="											<td align='left'><img src='https://global.molit.go.kr/img/국토교통부_국_좌우.png' width='100' height='29' alt='30103 세종특별자치시 도움6로 11 국토교통부 국제협력통상담당관실 044-201-3294'></td>";
	    	mailForm+="												<td>";
	    	mailForm+="															<p style='font-size:11px;line-height:1.0em;color:#666;'>";
	    	mailForm+="																30103 세종특별자치시 도움6로 11 국토교통부 국제협력통상담당관실 044-201-3294";
	    	mailForm+="															</p>";
	    	mailForm+="														</td>";
	    	mailForm+="										</tr>";
	    	mailForm+="										<tr>";
	    	mailForm+="											<td height='30'>&nbsp;</td>";
	    	mailForm+="										</tr>";
	    	mailForm+="									</tbody>";
	    	mailForm+="								</table>";
	    	mailForm+="							</td>";
	    	mailForm+="						</tr>";
	    	mailForm+="					</tbody>";
	    	mailForm+="				</table>";
	    	mailForm+="			</td>";
	    	mailForm+="		</tr>";
	    	mailForm+="		<tr>";
	    	mailForm+="			<td height='50'></td>";
	    	mailForm+="		</tr>";
	    	mailForm+="	</tbody>";
	    	mailForm+="</table>";
	    	mailForm+="</body>";
	    	mailForm+="</html>";

	    	
	    	return mailForm;
	    }
}


/**
 * 구글 사용자 메일 계정 아이디/패스 정보
 */
class SMTPAuthenticator extends javax.mail.Authenticator {
    public PasswordAuthentication getPasswordAuthentication() {
        String username =  "leesm1988@gmail.com"; // gmail 사용자;
        String password = "aldofkd0510"; // 패스워드;
        return new PasswordAuthentication(username, password);
    }
}