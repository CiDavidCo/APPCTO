<?php
	if (isset($_POST["scuola"]) &&
		isset($_POST["utente"]) &&
		isset($_POST["password"])){
			
		$scuola=$_REQUEST["scuola"];
        $password = $_REQUEST["password"];
        $utente= $_REQUEST["utente"];
                // chiamo il webservice
        $url = "https://web.spaggiari.eu/services/ws/wsExtAuth.php?wsdl";
        $client = new SoapClient( $url );
        $result = $client->__soapCall("wsExtAuth..ckAuth",    array(
                        'cid' =>$scuola,
                        'login' =>$utente,
                        'password' => $password));
        

        /*il webservice restituisce 3 array, il primo [0] contiene eventuali errori, 
		il secondo [1] la descrizione degli errori ed 
		infine il terzo [2] se i primi due sono vuoti conterrà le informazioni dell'account (dati di esempio):
    
*/
		if(!empty($result[0])){
                    //no login - restituito codice errore (il dettaglio dell'errore è nel secondo array $result[1] ma noi lo ignoriamo per un più generico messaggio predefinito di joomla)
                    print("errore !!<br>");
					print($result[1]);

		}
		else
		{
                    
			$info=$result[2];			//echo $result;
            //print_r($info);
            foreach($info as $key=>$value){
                echo $key . ":" . $value . "</br>";
            }
            // var_dump($info);
			// print("<br>");                
        }
	}

?>
<form method="post" action="#">
scuola <input type="text" name="scuola"> <br>
utente <input type="text" name="utente"> <br>
password <input type="password" name="password"> <br>
<input type="submit" value="ok">
</form>
