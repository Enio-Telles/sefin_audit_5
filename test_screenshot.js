import { chromium } from 'playwright';

(async () => {
  const browser = await chromium.launch();
  const page = await browser.newPage();

  // Try to go to the page if the server is running
  try {
    await page.goto('http://localhost:3000/analise-produtos', { timeout: 5000 });
    // enable dark mode
    await page.evaluate(() => {
      localStorage.setItem('theme', 'dark');
      document.documentElement.classList.add('dark');
    });
    await page.waitForTimeout(1000);
    await page.screenshot({ path: 'before.png' });
  } catch (e) {
    console.log("Server not running or page not found", e);
  }

  await browser.close();
})();
