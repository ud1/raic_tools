var
    request = require('request'),
    cheerio = require('cheerio');

var fs = require('fs');
var stream = fs.createWriteStream("R.json");

var processed = new Set();

stream.once('open', function(fd) {
	
	
	function parse(url, rowOut, done, finish) {
		request(url, function (error, response, body) {
			var $ = cheerio.load(body);
            var breakLoop = false;

			var rows = $('.gamesTable tbody tr');
            var processedRows = 0;
			rows.each(function (i) {
				var tds = $(this).children("td");
				var num = $(tds[0]).text().trim();
				var type = $(tds[1]).text().trim();
				
				var p1 = $($(tds[4]).find("a")[1]).text().trim();
				var p2 = $($(tds[4]).find("a")[3]).text().trim();
                var p3 = $($(tds[4]).find("a")[5]).text().trim();
                var p4 = $($(tds[4]).find("a")[7]).text().trim();
				
				var v = $($(tds[5])).text().trim().split(/\s+/);
				
				var pts1 = $($(tds[6]).find("div")[0]).text().trim();
				var pts2 = $($(tds[6]).find("div")[1]).text().trim();
                var pts3 = $($(tds[6]).find("div")[2]).text().trim();
                var pts4 = $($(tds[6]).find("div")[3]).text().trim();
				
				if (pts1 && pts2 && !processed.has(num))
				{
					var game = p3 ? {
						num: num,
						type: type,
						p: [p1, p2, p3, p4],
						v: [+v[0], +v[1], +v[2], +v[3]],
						pts: [+pts1, +pts2, +pts3, +pts4]
					} : {
                        num: num,
						type: type,
						p: [p1, p2],
						v: [+v[0], +v[1]],
						pts: [+pts1, +pts2]
                    };
                    
                    if (+num <= 1041190)
                    {
                        breakLoop = true;
                        return;
                    }
					
					rowOut(game);
					processed.add(num);
                    
                    ++processedRows;
				}
			});
            
            if (!processedRows)
                finish();
            else if (!breakLoop)
                done();
            else
                finish();
		})
	}

	//stream.write("[\n");
	
	var urls = [
        "https://russianaicup.ru/profile/ud1/allGames/page/",
        
        "https://russianaicup.ru/profile/Commandos/allGames/page/",
        "https://russianaicup.ru/profile/Recar/allGames/page/",
        "https://russianaicup.ru/profile/karliso/allGames/page/",
        "https://russianaicup.ru/profile/dgrachev28/allGames/page/",
        "https://russianaicup.ru/profile/LeeTiK/allGames/page/",
        "https://russianaicup.ru/profile/GreenTea/allGames/page/",
        "https://russianaicup.ru/profile/StarWix/allGames/page/",
        "https://russianaicup.ru/profile/ThermIt/allGames/page/",
        "https://russianaicup.ru/profile/Romka/allGames/page/",
        "https://russianaicup.ru/profile/kovi/allGames/page/",
        "https://russianaicup.ru/profile/morozec/allGames/page/",
        "https://russianaicup.ru/profile/aropan/allGames/page/",
        "https://russianaicup.ru/profile/lama/allGames/page/",
        "https://russianaicup.ru/profile/Milanin/allGames/page/",
        "https://russianaicup.ru/profile/T1024/allGames/page/",
        "https://russianaicup.ru/profile/TonyK/allGames/page/",
        "https://russianaicup.ru/profile/qubit/allGames/page/",
        "https://russianaicup.ru/profile/Leos/allGames/page/",
        "https://russianaicup.ru/profile/SiestaGuru/allGames/page/",
        "https://russianaicup.ru/profile/mixei4/allGames/page/",
        "https://russianaicup.ru/profile/cas/allGames/page/",
        "https://russianaicup.ru/profile/SilentNox/allGames/page/",
        "https://russianaicup.ru/profile/Lev/allGames/page/",
        "https://russianaicup.ru/profile/wala/allGames/page/",
        "https://russianaicup.ru/profile/cheeser/allGames/page/"
    ];
    
    var ui = 0;
	
	function processPage(page) {
		if (page > 150) {
			//stream.write("]\n");
			stream.end();
		} else {
			console.info("PAGE ", urls[ui] + page);
			
			parse(urls[ui] + page, function(game){
				stream.write(JSON.stringify(game));
				//stream.write(",\n");
                stream.write("\n");
			}, function() {
				processPage(page + 1);
			}, function() {
                ui++;
                
                if (ui < urls.length)
                    processPage(1);
			});
		}
	}
	
	processPage(1);
	
});
