#!/bin/bash

set -ex  # 'set -e' detiene el script en errores, y 'set -x' permite ver los comandos ejecutados
COMMAND="/usr/sbin/asterisk -T -U omnileads -p -vvvf"

# Configurar zona horaria
echo "**[omlacd] Setting localtime"
rm -f /etc/localtime  # Elimina sin error si no existe
ln -s "/usr/share/zoneinfo/${TZ:-UTC}" /etc/localtime  # Establece UTC si no está definida $TZ

# Configuración de ARI
if [[ -n "${ASTERISK_USER}" && -n "${ASTERISK_PASS}" ]]; then
    sed -i "s/ariuser/${ASTERISK_USER}/g" /etc/asterisk/ari.conf
    sed -i "s/aripassword/${ASTERISK_PASS}/g" /etc/asterisk/ari.conf
fi

# Configuración de OMLeads ACD PJSIP peer
if [[ -n "${OMLACD_SIP_SERVER}" ]]; then
    sed -i "s/acd:5260/${OMLACD_SIP_SERVER}/g" /etc/asterisk/pjsip_wizard_omlacd.conf
fi

# Configuración de NAT & SIP
if [[ -n "${SIP_NAT_ADDR}" ]]; then
    sed -i "s#;external_media_address=localhost#external_media_address=${SIP_NAT_ADDR}#g" /etc/asterisk/pjsip.conf
    sed -i "s#;external_signaling_address=localhost#external_signaling_address=${SIP_NAT_ADDR}#g" /etc/asterisk/pjsip.conf    
else
    echo "NAT mode selected was not found !!!"
fi  

# Configuración opcional para SIP Gateway PSTN 
if [[ -n "${PSTNGW_REGISTER}" && "${PSTNGW_REGISTER}" == "yes" ]]; then
    sed -i "s/sends_registrations=no/sends_registrations=yes/g" /etc/asterisk/pjsip_wizard_pstngw.conf
fi

# Autenticacion SIP hacia PSTN GW
if [[ -n "${PSTNGW_SIP_AUTH}" && "${PSTNGW_SIP_AUTH}" == "yes" ]]; then
    sed -i "s/sends_auth=no/sends_auth=yes/g" /etc/asterisk/pjsip_wizard_pstngw.conf
    sed -i "s#;endpoint/from_user=pstn_gw_user#endpoint/from_user=${PSTNGW_SIP_USER}#g" /etc/asterisk/pjsip_wizard_pstngw.conf
    sed -i "s#;outbound_auth/username=pstn_gw_user#outbound_auth/username=${PSTNGW_SIP_USER}#g" /etc/asterisk/pjsip_wizard_pstngw.conf
    sed -i "s#;outbound_auth/password=pstn_gw_pass#outbound_auth/password=${PSTNGW_SIP_PASS}#g" /etc/asterisk/pjsip_wizard_pstngw.conf
fi

# Reemplazar el hostname del PSTN si está definido
if [[ -n "${PSTNGW_HOSTNAME}" ]]; then
    sed -i "s/tel_gateway:5260/${PSTNGW_HOSTNAME}/g" /etc/asterisk/pjsip_wizard_pstngw.conf
else
    sed -i "s/tel_gateway:5260/${OMLACD_SIP_SERVER}/g" /etc/asterisk/pjsip_wizard_pstngw.conf
fi


# Iniciar el servidor Asterisk
echo "**[omlacd] Initializing Asterisk"
exec ${COMMAND}
