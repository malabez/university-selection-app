//require footable/footable.all.min.js
//require slick/slick.min.js
//= require lib/axios/dist/axios.standalone.js
//= require lib/CryptoJS/rollups/hmac-sha256.js
//= require lib/CryptoJS/rollups/sha256.js
//= require lib/CryptoJS/components/hmac.js
//= require lib/CryptoJS/components/enc-base64.js
//= require lib/url-template/url-template.js
//= require lib/apiGatewayCore/sigV4Client.js
//= require lib/apiGatewayCore/apiGatewayClient.js
//= require lib/apiGatewayCore/simpleHttpClient.js
//= require lib/apiGatewayCore/utils.js
//= require searchApigClient.js

function test(university_name, program_name, user_email){
  var apigClient = apigClientFactory.newClient({
    accessKey: "",
    secretKey: "",
    region: "us-east-1"
  });
  var params = {};
  var body = {
    name: university_name,
    session_id: user_email,
    browser: program_name 
  };
  var additionalParams = {};

  apigClient
    .ecommerceresourcePost(params, body, additionalParams)
    .then(function(result) {
      // alert(result.data.body);
      console.log(result.data.body);
    })
    .catch(function(result) {
      // alert(result.message);
      console.log(result.message);
    });
  
};